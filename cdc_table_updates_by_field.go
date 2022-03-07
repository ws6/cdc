package cdc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ws6/klib"

	"context"

	"github.com/ws6/calculator/transformation"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/dlock"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"

	"github.com/ws6/msi"
)

func init() {
	transformation.RegisterType(new(FieldIncrementatlRefresh))
}

type FieldIncrementatlRefresh struct {
	db    *msi.Msi
	cfg   *confighelper.SectionConfig
	progr progressor.Progressor
	dl    *dlock.Dlock
}

func (self *FieldIncrementatlRefresh) Type() string {
	return `FieldIncrementatlRefresh`
}
func (self *FieldIncrementatlRefresh) Name() string {
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()
}
func (self *FieldIncrementatlRefresh) Close() error {

	self.dl.Close()
	self.progr.Close()
	return nil
}

func (self *FieldIncrementatlRefresh) NewTransformer(cfg *confighelper.SectionConfig) (transformation.Transformer, error) {
	ret := new(FieldIncrementatlRefresh)
	ret.cfg = cfg
	//TODO open database
	var err error
	key := cfg.SectionName
	ret.db, err = createIfNotExistDb(key, cfg.ConfigMap)

	if err != nil {
		return nil, err
	}

	//add fields
	dlockConfigSection, ok := ret.cfg.ConfigMap[`dlock_config_section`]
	if !ok {
		return nil, fmt.Errorf(`no dlock_config_section`)
	}
	dlockCfg, err := ret.cfg.Configer.GetSection(dlockConfigSection)
	if err != nil {
		return nil, fmt.Errorf(`GetSection(%s):%s`, dlockConfigSection, err.Error())
	}
	ret.dl, err = dlock.NewDlock(dlockCfg)
	if err != nil {
		return nil, fmt.Errorf(`NewDlock:%s`, err.Error())
	}

	progressorSectionName := self.cfg.ConfigMap[`progressor`]
	progr, err := extraction.InitProgrssorFromConfigSection(self.cfg.Configer, progressorSectionName)
	if err != nil {
		return nil, fmt.Errorf(`InitProgrssorFromConfigSection:%s`, err.Error())
	}
	self.progr = progr
	return ret, nil
}

func (self *FieldIncrementatlRefresh) GetLimit() int {
	if s, ok := self.cfg.ConfigMap[`fetch_limit`]; ok {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_LIMIT
}
func (self *FieldIncrementatlRefresh) GetPrimaryKeyName(ctx context.Context, f *TableField) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT 
     KU.table_name as table_name
    ,column_name as primary_key_column
	 
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 

INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
     AND KU.table_name='%s'
	 AND KU.TABLE_SCHEMA = '%s'
	order by KU.table_name,  ku.ORDINAL_POSITION 
	`,
		f.TableName,
		f.SchameName,
	)
	founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil
	}
	ret := []string{}
	for _, found := range founds {
		topush, err := msi.ToString(found[`primary_key_column`])
		if err != nil {
			return nil, err
		}
		ret = append(ret, topush)
		//unable to handle multiple primary_key_col if exists
	}
	return ret, nil
}

func (self *FieldIncrementatlRefresh) GenerateIncrementalRefreshQueryWithTime(f *TableField, p time.Time, limit int, offset int64) string {
	timefilter := ""
	timelayout := "2006-01-02 15:04:05"
	if f.SecondScale > 0 {
		zeros := fmt.Sprintf("%0*d", f.SecondScale, 0)
		timelayout = fmt.Sprintf("%s.%s", timelayout, zeros)
	}

	if !p.IsZero() {
		ts := p.Format(timelayout)
		timefilter = fmt.Sprintf(`AND [%s]>'%s'`, f.ColumnName, ts)
	}
	offsetlimitStr := fmt.Sprintf(` 
		OFFSET %d ROWS
        FETCH NEXT %d ROWS ONLY`, offset, limit)
	query := `
	SELECT
	*
	FROM [%s].[%s]
	WHERE 1=1
	-- AND [%s] is not null
	%s
	ORDER BY [%s] ASC
	%s
	`
	query = fmt.Sprintf(query,
		f.SchameName,
		f.TableName,
		f.ColumnName,
		timefilter,
		f.ColumnName,
		offsetlimitStr,
	)

	return query
}

func (self *FieldIncrementatlRefresh) GenerateItem(ctx context.Context, f *TableField, ps progressSaver, p *progressor.Progress, recv chan<- *klib.Message) error {

	limit := self.GetLimit() //todo load from config
	offset := int64(0)
	primaryKeyNames, err := self.GetPrimaryKeyName(ctx, f)
	if err != nil {
		fmt.Println(`GetPrimaryKeyName`, err.Error())
		return err
	}
	begin := time.Time{}
	if p != nil {
		begin = p.Timestamp
	}
	pushed := int64(0)
	for {
		query := self.GenerateIncrementalRefreshQueryWithTime(f, begin, limit, offset)
		fmt.Println(`sending query`)

		offset += int64(limit)
		founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
		fmt.Println(query)
		if err != nil {
			fmt.Println(query)
			return err
		}
		if len(founds) == 0 {
			return nil
		}
		sent := 0
		for _, found := range founds {
			topush := new(klib.Message)
			evt := msiToEventSpec(primaryKeyNames, f, found)
			topush.Value, err = json.Marshal(evt)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case recv <- topush:
				sent++
				//update progress
				if t1, ok := found[f.ColumnName].(time.Time); ok {
					p.Timestamp = t1
				}
				pushed++
				if (pushed)%int64(limit) == 0 {
					if err := ps(p); err != nil {
						fmt.Println(`progressSaver`, err.Error())
					}
				}
			}
		}
		fmt.Println(`sent`, sent)
		if len(founds) < limit {
			return nil
		}

	}

	return nil

}

func (self *FieldIncrementatlRefresh) Transform(ctx context.Context, eventMsg *klib.Message, recv chan<- *klib.Message) error {
	//!!!each transformer could be running long so keep open then close the progressor here
	progr := self.progr
	//TODO move this to NewIncref

	tableField := new(TableField)
	if err := json.Unmarshal(eventMsg.Value, tableField); err != nil {
		return err
	}

	//build query with progressor
	f := tableField
	fmt.Println(`field is   in filter`, f)

	k := fmt.Sprintf(`%s.%s.%s`, f.SchameName, f.TableName, f.ColumnName)
	ps := func(_k string) progressSaver {
		return func(_p *progressor.Progress) error {
			fmt.Println(`saving progress`, k, _p)
			return progr.SaveProgress(_k, _p)
		}
	}(k)
	//TODO add dlock
	dmux := self.dl.NewMutex(ctx, k)
	if err := dmux.Lock(); err != nil {
		return fmt.Errorf(`dmux.Lock():%s`, err.Error())
	}

	defer dmux.Unlock()

	prog, err := progr.GetProgress(k)
	if err != nil {
		if err != progressor.NOT_FOUND_PROGRESS {
			fmt.Println(`GetProgress`, err.Error())
			return fmt.Errorf(`GetProgress:%s`, err.Error())
		}
	}
	if prog == nil {
		prog = new(progressor.Progress)
	}

	if err := self.GenerateItem(ctx, f, ps, prog, recv); err != nil {
		fmt.Println(`GenerateItem`, err.Error())
		return fmt.Errorf(`GenerateItem:%s`, err.Error())
	}

	if err := progr.SaveProgress(k, prog); err != nil {
		fmt.Println(`SaveProgress`, err.Error())
		return fmt.Errorf(`SaveProgress:%s`, err.Error())
	}

	//update it

	return nil
}
