package mssql

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ws6/cdc/specs"
	"github.com/ws6/klib"

	"github.com/ws6/calculator/transformation"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/dlock"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"

	"github.com/ws6/msi"
)

const (
	DEFAULT_LIMIT = 1000
)

func init() {
	transformation.RegisterType(new(FieldIncrementatlRefresh))
}

type progressSaver func(*progressor.Progress) error

type FieldIncrementatlRefresh struct {
	db                 *msi.Msi
	cfg                *confighelper.SectionConfig
	progr              progressor.Progressor
	dl                 *dlock.Dlock
	addMetaFieldToSpec bool
}

func (self *FieldIncrementatlRefresh) Type() string {
	return `PQFieldIncrementatlRefresh`
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
	connstr := ret.cfg.ConfigMap[`conn`]
	dbname := ret.cfg.ConfigMap[`db_name`]
	// connstr, err = pq.ParseURL(connstr)
	// if err != nil {
	// 	return nil, fmt.Errorf(`pq.ParseURL:%s`, err.Error())
	// }

	ret.db, err = msi.NewDb(msi.POSTGRES, connstr, dbname, ``)
	if err != nil {
		return nil, fmt.Errorf(`NewDb:%s`, err.Error())
	}
	ret.addMetaFieldToSpec = true //default off
	if removeMetaFieldStr := ret.cfg.ConfigMap[`remove_meta_field`]; removeMetaFieldStr == `true` {
		ret.addMetaFieldToSpec = false
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

	progressorSectionName := ret.cfg.ConfigMap[`progressor`]
	progr, err := extraction.InitProgrssorFromConfigSection(ret.cfg.Configer, progressorSectionName)
	if err != nil {
		return nil, fmt.Errorf(`InitProgrssorFromConfigSection:%s`, err.Error())
	}
	ret.progr = progr
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

func (self *FieldIncrementatlRefresh) GenerateIncrementalRefreshQueryWithInteger(f *TableField, begin int64, limit int, offset int64) string {
	timefilter := fmt.Sprintf(`
	 AND  "%s" > %d
	`, f.ColumnName, begin,
	)

	offsetlimitStr := fmt.Sprintf(` 
		LIMIT  %d  OFFSET %d`, limit, offset)
	query := `
	SELECT
	*
	FROM  %s."%s"
	WHERE 1=1
	 
	%s
	ORDER BY "%s" ASC
	%s
	`
	query = fmt.Sprintf(query,
		f.SchameName,
		f.TableName,
		timefilter,
		f.ColumnName,
		offsetlimitStr,
	)

	return query
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

func (self *FieldIncrementatlRefresh) msiToEventSpec(primaryKeyNames []string, f *TableField, m map[string]interface{}) *specs.EventMessage {
	ret := new(specs.EventMessage)
	ret.ResourceType = fmt.Sprintf(`[%s].[%s]`, f.SchameName, f.TableName)
	ret.EventType = `Update`
	resourceId := []string{}
	for _, pk := range primaryKeyNames {
		resourceId = append(resourceId, fmt.Sprintf("%v", m[pk]))
	}
	ret.ResourceId = strings.Join(resourceId, "-")
	ret.ResourceKey = strings.Join(primaryKeyNames, "-")
	if self.addMetaFieldToSpec {
		ret.MetaData = m
	}

	event := fmt.Sprintf(`%s-%s-%v`, ret.ResourceType, ret.ResourceId, m[f.ColumnName])
	ret.EventId = fmt.Sprintf("%x", md5.Sum([]byte(event)))
	ret.DateCreated = fmt.Sprintf("%v", m[f.ColumnName]) // or time.Now if configured
	if m[f.ColumnName] == nil {
		ret.DateCreated = ""
	}
	ret.FieldChanges = make(map[string]*specs.Change)
	ret.FieldChanges[f.ColumnName] = &specs.Change{
		NewValue: ret.DateCreated,
	}
	return ret
}

func (self *FieldIncrementatlRefresh) GenerateItem(ctx context.Context, f *TableField, ps progressSaver, p *progressor.Progress, recv chan<- *klib.Message) error {

	if strings.Contains(f.DataType, "time") {
		return self.GenerateItemByTime(ctx, f, ps, p, recv)
	}

	switch f.DataType {
	case "bigint":
		return self.GenerateItemByInt(ctx, f, ps, p, recv)
	case "integer":
		return self.GenerateItemByInt(ctx, f, ps, p, recv)
	}
	return nil
}

func (self *FieldIncrementatlRefresh) GenerateItemByInt(ctx context.Context, f *TableField, ps progressSaver, p *progressor.Progress, recv chan<- *klib.Message) error {

	pushed := int64(0)
	defer func() {
		fmt.Println(`GenerateItemByInt exit; pushed=`, pushed)
	}()
	limit := self.GetLimit() //todo load from config
	offset := int64(0)
	primaryKeyNames, err := self.GetPrimaryKeyName(ctx, f)
	if err != nil {
		fmt.Println(`GetPrimaryKeyName`, err.Error())
		return err
	}
	begin := p.Number

	for {
		//TODO check what field dataType is to choose a query
		query := self.GenerateIncrementalRefreshQueryWithInteger(f, begin, limit, offset)
		// fmt.Println(query)
		offset += int64(limit)
		founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)

		if err != nil {
			fmt.Println(query)
			return err
		}

		if len(founds) == 0 {
			fmt.Println(`zero found`)
			return nil
		}

		sent := 0
		for _, found := range founds {

			topush := new(klib.Message)
			evt := self.msiToEventSpec(primaryKeyNames, f, found)
			topush.Value, err = json.Marshal(evt)
			// topush.Key=fmt.Sprintf(`%d`,f)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case recv <- topush:
				sent++
				//update progress
				if n1, err := msi.ToInt64(found[f.ColumnName]); err == nil {
					p.Number = n1
				}
				pushed++

			}

			if (pushed)%int64(limit) == 0 {
				if err := ps(p); err != nil {
					fmt.Println(`progressSaver`, err.Error())
				}
			}
		}

		if len(founds) < limit {
			return nil
		}

	}

	return nil

}

func (self *FieldIncrementatlRefresh) GenerateItemByTime(ctx context.Context, f *TableField, ps progressSaver, p *progressor.Progress, recv chan<- *klib.Message) error {

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
		//TODO check what field dataType is to choose a query
		query := self.GenerateIncrementalRefreshQueryWithTime(f, begin, limit, offset)
		fmt.Println(`sending query`)

		offset += int64(limit)
		founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)

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
			evt := self.msiToEventSpec(primaryKeyNames, f, found)
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

			}
			if (pushed)%int64(limit) == 0 {
				if err := ps(p); err != nil {
					fmt.Println(`progressSaver`, err.Error())
				}
			}
		}

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
	//enrich the field
	// f, err := UpdateFieldInfo(ctx, self.db, f0)
	// if err != nil {
	// 	return fmt.Errorf(`UpdateFieldInfo:%s`, err.Error())
	// }
	fmt.Println(`field updated`, f)
	f.MaxValue = f.MaxValue //copy

	k := fmt.Sprintf(`%s.%s.%s`, f.SchameName, f.TableName, f.ColumnName)
	ps := func(_k string) progressSaver {
		return func(_p *progressor.Progress) error {
			fmt.Println(`progressSaver`, _p)
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
