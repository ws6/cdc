package cdc

//cdc_table_update_transformer.go take the event mess from TimeCDC to interact with each individual tab;e updates
import (
	"context"

	"encoding/json"
	"fmt"

	"strings"

	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/transformation"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/klib"
	"github.com/ws6/msi"
)

const (
	DEFAULT_LIMIT = 1000
)

func init() {
	// transformation.RegisterType(new(FieldUpdated))
}

//transform from table update events to table fields updates events
type FieldUpdated struct {
	db  *msi.Msi
	cfg *confighelper.SectionConfig
}

//InitProgrssorFromConfigSection
func (self *FieldUpdated) Close() error {
	return nil
}
func (self *FieldUpdated) Type() string {
	return `FieldUpdated`
}
func (self *FieldUpdated) Name() string {
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()
}
func (self *FieldUpdated) NewTransformer(cfg *confighelper.SectionConfig) (transformation.Transformer, error) {

	ret := new(FieldUpdated)
	ret.cfg = cfg
	//TODO open database
	var err error

	key := cfg.SectionName

	ret.db, err = createIfNotExistDb(key, cfg.ConfigMap)

	if err != nil {
		return nil, err
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

func GetDateTimeFieldQuery(SchemaName, TableName string) string {
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
		TableName,
		SchemaName,
	)
}

func (self *TableUpdateEvent) GetDateTimeFieldQuery() string {
	return GetDateTimeFieldQuery(self.SchemaName, self.TableName)
}

func (self *FieldUpdated) GetDateTimeField(ctx context.Context, e *TableUpdateEvent) ([]map[string]interface{}, error) {
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
	MaxValue    string `json:"max_value"`
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

func (self *FieldUpdated) TableFields() []string {
	s, ok := self.cfg.ConfigMap[`table_fields_include`]
	if !ok {
		return []string{}
	}
	return strings.Split(s, ",")
}

func (self *FieldUpdated) IsAllowedTableFields(f *TableField) bool {
	tableFields := self.TableFields()
	if len(tableFields) == 0 {
		return true //!!!if user didnt do it. it leaves a usage to pullout everything
	}

	k := fmt.Sprintf(`%s.%s.%s`, f.SchameName, f.TableName, f.ColumnName)
	for _, tf := range tableFields {
		if strings.ToLower(k) == strings.ToLower(tf) {
			return true
		}
	}

	return false

}

type progressSaver func(*progressor.Progress) error

//Transform

func (self *FieldUpdated) Transform(ctx context.Context, eventMsg *klib.Message, recv chan<- *klib.Message) error {

	tableEvent := new(TableUpdateEvent)
	if err := json.Unmarshal(eventMsg.Value, tableEvent); err != nil {
		return err
	}
	//get datetime fields
	fields, err := self.GetDateTimeField(ctx, tableEvent)
	if err != nil {
		return err
	}

	for _, field := range fields {
		f, err := tableFieldfromMsi(field)
		if err != nil {
			return err
		}
		if !self.IsAllowedTableFields(f) {
			continue
		}
		topush := new(klib.Message)
		topush.Value, err = json.Marshal(f)
		if err != nil {

			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case recv <- topush:
			continue
		}

	}

	return nil
}
