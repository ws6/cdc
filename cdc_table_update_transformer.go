package cdc

//cdc_table_update_transformer.go take the event mess from TimeCDC to interact with each individual tab;e updates
import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/transformation"
	"github.com/ws6/calculator/utils/confighelper"
	"github.com/ws6/calculator/utils/dbhelper"
	"github.com/ws6/cdc/specs"
	"github.com/ws6/klib"
	"github.com/ws6/msi"
)

const (
	DEFAULT_LIMIT = 1000
)

func init() {
	transformation.RegisterType(new(TimeCDCByTable))
}

type TimeCDCByTable struct {
	db  *msi.Msi
	cfg *confighelper.SectionConfig
	p   progressor.Progressor
}

//InitProgrssorFromConfigSection
func (self *TimeCDCByTable) Close() error {
	self.p.Close()
	return self.db.Close()
}
func (self *TimeCDCByTable) Type() string {
	return `TimeCDCByTable`
}
func (self *TimeCDCByTable) Name() string {
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()
}
func (self *TimeCDCByTable) NewTransformer(cfg *confighelper.SectionConfig) (transformation.Transformer, error) {

	ret := new(TimeCDCByTable)
	ret.cfg = cfg
	//TODO open database
	var err error
	ret.db, err = dbhelper.GetMSDB(cfg.ConfigMap)
	if err != nil {
		return nil, err
	}
	progressorSectionName := ret.cfg.ConfigMap[`progressor`]
	ret.p, err = extraction.InitProgrssorFromConfigSection(cfg.Configer, progressorSectionName)
	if err != nil {
		return nil, fmt.Errorf(`InitProgrssorFromConfigSection:%s`, err.Error())
	}

	return ret, nil
}

//the auto generated last modifed table event shall be like this
// {
//   "create_date": "2007-05-14T11:15:40.3Z",
//   "filestream_data_space_id": null,
//   "has_replication_filter": false,
//   "has_unchecked_assembly_data": false,
//   "is_filetable": false,
//   "is_merge_published": false,
//   "is_ms_shipped": false,
//   "is_published": false,
//   "is_replicated": false,
//   "is_schema_published": false,
//   "is_sync_tran_subscribed": false,
//   "is_tracked_by_cdc": false,
//   "large_value_types_out_of_row": false,
//   "lob_data_space_id": 0,
//   "lock_escalation": 0,
//   "lock_escalation_desc": "TABLE",
//   "lock_on_bulk_load": false,
//   "max_column_id_used": 10,
//   "modify_date": "2014-09-21T17:45:14.237Z",
//   "name": "ELB_LOGIC",
//   "object_id": 1746105261,
//   "parent_object_id": 0,
//   "principal_id": null,
//   "schema_id": 1,
//   "schema_name": "dbo",
//   "text_in_row_limit": 0,
//   "type": "U ",
//   "type_desc": "USER_TABLE",
//   "uses_ansi_nulls": true
// }

type TableUpdateEvent struct {
	CreateDate string `json:"create_date"`
	ModifyDate string `json:"modify_date"`
	Type       string `json:"type"`
	TypeDesc   string `json:"type_desc"`
	TableName  string `json:"name"`
	SchemaName string `json:"schema_name"`
}

func (self *TableUpdateEvent) GetProgressorKey(fieldName string) string {
	return fmt.Sprintf(`%s.%s.%s`, self.SchemaName, self.TableName, fieldName)
}

func (self *TableUpdateEvent) GetDateTimeFieldQuery() string {
	return fmt.Sprintf(
		`select schema_name(t.schema_id)    as schema_name
      ,t.name as table_name ,
       c.column_id,
       c.name as column_name,
       type_name(user_type_id) as data_type,
       scale as second_scale
from sys.columns c
join sys.tables t
     on t.object_id = c.object_id
where type_name(user_type_id) in ('date', 'datetimeoffset', 
      'datetime2', 'smalldatetime', 'datetime', 'time')
    and t.name='%s'
    and schema_name(t.schema_id) ='%s' 
order by t.name,
         c.column_id;`,
		self.TableName,
		self.SchemaName,
	)
}

func (self *TimeCDCByTable) GetDateTimeField(ctx context.Context, e *TableUpdateEvent) ([]map[string]interface{}, error) {
	//TODO add filters
	query := e.GetDateTimeFieldQuery()
	return self.db.MapContext(ctx, self.db.Db, query, nil)
}

type TableField struct {
	SchameName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	ColumnName  string `json:"column_name"`
	DataType    string `json:"data_type"`
	SecondScale int    `json:"second_scale"`
}

func tableFieldfromMsi(m map[string]interface{}) (*TableField, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	ret := new(TableField)
	if err := json.Unmarshal(b, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (self *TimeCDCByTable) GetPrimaryKeyName(ctx context.Context, f *TableField) (string, error) {
	query := fmt.Sprintf(
		`SELECT 
     KU.table_name as table_name
    ,column_name as primary_key_column
	 
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 

INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
     AND KU.table_name='%s'
	 AND KU.TABLE_SCHEMA = '%s'`,
		f.TableName,
		f.SchameName,
	)
	founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
	if err != nil {
		fmt.Println(err.Error())
		return "", nil
	}
	for _, found := range founds {
		return msi.ToString(found[`primary_key_column`])
		//unable to handle multiple primary_key_col if exists
	}
	return "", nil
}

func (self *TimeCDCByTable) GenerateIncrementalRefreshQueryWithTime(f *TableField, p time.Time, limit int, offset int64) string {
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
	%s
	ORDER BY %s ASC
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

func msiToEventSpec(primaryKeyName string, f *TableField, m map[string]interface{}) *specs.EventMessage {
	ret := new(specs.EventMessage)
	ret.ResourceType = fmt.Sprintf(`[%s].[%s]`, f.SchameName, f.TableName)
	ret.ResoureId = fmt.Sprintf("%v", m[primaryKeyName])
	ret.MetaData = m
	event := fmt.Sprintf(`%s-%s-%v`, ret.ResourceType, ret.ResoureId, m[f.ColumnName])
	ret.EventId = fmt.Sprintf("%x", md5.Sum([]byte(event)))
	ret.DateCreated = fmt.Sprintf("%v", m[f.ColumnName]) // or time.Now if configured
	ret.FieldChanges = make(map[string]*specs.Change)
	ret.FieldChanges[f.ColumnName] = &specs.Change{
		NewValue: ret.DateCreated,
	}
	return ret
}

func (self *TimeCDCByTable) GetLimit() int {
	if s, ok := self.cfg.ConfigMap[`fetch_limit`]; ok {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_LIMIT
}

func (self *TimeCDCByTable) GenerateItem(ctx context.Context, f *TableField, p *progressor.Progress, recv chan *klib.Message) error {
	limit := self.GetLimit() //todo load from config
	offset := int64(0)
	primaryKeyName, err := self.GetPrimaryKeyName(ctx, f)
	if err != nil {
		return err
	}
	begin := p.Timestamp
	for {
		query := self.GenerateIncrementalRefreshQueryWithTime(f, begin, limit, offset)
		offset += int64(limit)
		founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
		if err != nil {
			return err
		}
		if len(founds) == 0 {
			return nil
		}
		for _, found := range founds {
			topush := new(klib.Message)
			evt := msiToEventSpec(primaryKeyName, f, found)
			topush.Value, err = json.Marshal(evt)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case recv <- topush:
				//update progress
				if t1, ok := found[f.ColumnName].(time.Time); ok {
					p.Timestamp = t1
				}
				continue
			}
		}
		if len(founds) < limit {
			return nil
		}

	}

	return nil

}

//Transform
//TODO add table and field filters
func (self *TimeCDCByTable) Transform(ctx context.Context, eventMsg *klib.Message) (chan *klib.Message, error) {
	ret := make(chan *klib.Message)
	tableEvent := new(TableUpdateEvent)
	if err := json.Unmarshal(eventMsg.Value, tableEvent); err != nil {
		return nil, err
	}
	//get datetime fields
	fields, err := self.GetDateTimeField(ctx, tableEvent)
	if err != nil {
		return nil, err
	}
	//??use waitgroup?
	go func() {
		defer close(ret)
		//build query with progressor

		for _, field := range fields {
			//get progress make query

			//send items into chan
			f, err := tableFieldfromMsi(field)
			if err != nil {
				fmt.Println(`tableFieldfromMsi`, err.Error())
				return

			}
			k := fmt.Sprintf(`%s.%s.%s`, f.SchameName, f.TableName, f.ColumnName)
			prog, err := self.p.GetProgress(k)
			if err != nil {
				if err != progressor.NOT_FOUND_PROGRESS {
					fmt.Println(`GetProgress`, err.Error())
					return
				}
			}
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if err := self.GenerateItem(ctx, f, prog, ret); err != nil {
				fmt.Println(`GenerateItem`, err.Error())
				return
			}

			if err := self.p.SaveProgress(k, prog); err != nil {
				fmt.Println(`SaveProgress`, err.Error())
				return
			}
			//update it
		}

	}()

	return ret, nil
}
