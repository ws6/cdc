package cdc

//cdc_time_stamps.go collect data/timestamps field for database
//assume the time is incrementally added into the database.
//it shall not be used such a date-of-birth field, which is not following a incremental refresh concept
import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/utils/confighelper"
	"github.com/ws6/calculator/utils/dbhelper"

	"github.com/ws6/msi"
)

func init() {

	extraction.RegisterType(new(TableUpdated))
}

type TableUpdated struct {
	db  *msi.Msi
	cfg *confighelper.SectionConfig
}

func (self *TableUpdated) Close() error {
	if self.db != nil {
		return self.db.Db.Close()
	}
	return nil
}
func (self *TableUpdated) Type() string {
	return `TableUpdated`
}
func (self *TableUpdated) Name() string {
	//TODO from config
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()

}
func (self *TableUpdated) SaveProgressOnFail(error) bool {
	return false
}
func (self *TableUpdated) UpdateProgress(item map[string]interface{}, p *progressor.Progress) error {

	if t0, ok := item[`modify_date`].(time.Time); ok {

		p.Timestamp = t0
		return nil
	}
	modify_date, err := msi.ToTime(item[`modify_date`])
	if err != nil {
		fmt.Println(item)
		fmt.Println(`modify_date`, err.Error())
		return nil
	}
	if modify_date != nil {
		p.Timestamp = *modify_date
	}
	return nil
}

func (self *TableUpdated) NewIncref(cfg *confighelper.SectionConfig) (extraction.Incref, error) {
	ret := new(TableUpdated)
	ret.cfg = cfg
	//TODO open database
	var err error
	ret.db, err = dbhelper.GetMSDB(cfg.ConfigMap)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (self *TableUpdated) getTablesInclude() []string {
	tablesIncludeStr := self.cfg.ConfigMap[`tables_names_include`]
	if tablesIncludeStr == "" {
		return nil
	}
	sp := strings.Split(tablesIncludeStr, ",")
	return sp
}
func (self *TableUpdated) makeQuery(p *progressor.Progress) string {
	timefitler := ""
	if !p.Timestamp.IsZero() {
		timefitler = fmt.Sprintf(`AND modify_date>'%s'`, dbhelper.ToSQLDatetimeStringLocalMilliSecond(p.Timestamp))
	}
	tablesIncludeFilter := ""
	tablesInclude := self.getTablesInclude()
	if len(tablesInclude) > 0 {
		ts := []string{}
		for _, t := range tablesInclude {
			ts = append(ts, fmt.Sprintf(`'%s'`, t))
		}
		tablesIncludeFilter = fmt.Sprintf(`AND name in ( %s )`, strings.Join(ts, ","))
	}
	return fmt.Sprintf(`
	SELECT  schema_name(schema_id) as schema_name , * FROM sys.tables
where 1=1 
%s
%s
order by modify_date asc `,
		timefitler,
		tablesIncludeFilter,
	)
}
func (self *TableUpdated) GetChan(ctx context.Context, p *progressor.Progress) (chan map[string]interface{}, error) {
	query := self.makeQuery(p)

	ret := make(chan map[string]interface{})
	go func() {
		defer close(ret)
		founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
		if err != nil {
			fmt.Println(`MapContext`, err.Error())
			return
		}
		for _, found := range founds {
			schema_name, _ := msi.ToString(found[`schema_name`])
			table_name, _ := msi.ToString(found[`name`])
			if !self.IsAllowedTable(schema_name, table_name) {
				continue
			}

			ret <- found
		}
	}()

	return ret, nil
}

func (self *TableUpdated) Tables() []string {
	s, ok := self.cfg.ConfigMap[`tables_include`]
	if !ok {
		return []string{}
	}
	return strings.Split(s, ",")
}

func (self *TableUpdated) IsAllowedTable(schema, tableName string) bool {
	tableFields := self.Tables()
	if len(tableFields) == 0 {
		return true //!!!if user didnt do it. it leaves a usage to pullout everything
	}

	k := fmt.Sprintf(`%s.%s`, schema, tableName)
	for _, tf := range tableFields {
		if strings.ToLower(k) == strings.ToLower(tf) {
			return true
		}
	}

	return false

}
