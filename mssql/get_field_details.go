package mssql

import (
	"context"
	"fmt"

	"github.com/ws6/msi"
)

//get_field_details.go auto fill the scale etc info

func UpdateFieldInfo(ctx context.Context, db *msi.Msi, field *TableField) (*TableField, error) {

	query := fmt.Sprintf(
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
    and c.name = '%s'
order by t.name,
         c.column_id;`,
		field.TableName,
		field.SchameName,
		field.ColumnName,
	)
	founds, err := db.MapContext(ctx, db.Db, query, nil)
	if err != nil {
		return nil, err
	}
	if len(founds) == 0 {
		return nil, fmt.Errorf(`no such field`)
	}
	if len(founds) > 1 {
		return nil, fmt.Errorf(`too may fields found`)
	}

	found := founds[0]
	return tableFieldfromMsi(found)
}
