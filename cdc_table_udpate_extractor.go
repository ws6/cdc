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

	extraction.RegisterType(new(TimeCDC))
}

type TimeCDC struct {
	db  *msi.Msi
	cfg *confighelper.SectionConfig
}

func (self *TimeCDC) Close() error {
	if self.db != nil {
		return self.db.Db.Close()
	}
	return nil
}
func (self *TimeCDC) Type() string {
	return `TimeCDC`
}
func (self *TimeCDC) Name() string {
	//TODO from config
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()

}
func (self *TimeCDC) SaveProgressOnFail(error) bool {
	return false
}
func (self *TimeCDC) UpdateProgress(item map[string]interface{}, p *progressor.Progress) error {
	s, e0 := msi.ToString(item[`modify_date`])
	if e0 != nil {

		fmt.Println(e0.Error())
	}
	fmt.Println(s)
	if t0, ok := item[`modify_date`].(time.Time); ok {
		fmt.Println(`it is a time`)
		fmt.Println(t0)
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

func (self *TimeCDC) NewIncref(cfg *confighelper.SectionConfig) (extraction.Incref, error) {
	ret := new(TimeCDC)
	ret.cfg = cfg
	//TODO open database
	var err error
	ret.db, err = dbhelper.GetMSDB(cfg.ConfigMap)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (self *TimeCDC) getTablesInclude() []string {
	tablesIncludeStr := self.cfg.ConfigMap[`tables_include`]
	if tablesIncludeStr == "" {
		return nil
	}
	sp := strings.Split(tablesIncludeStr, ",")
	return sp
}
func (self *TimeCDC) makeQuery(p *progressor.Progress) string {
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
func (self *TimeCDC) GetChan(ctx context.Context, p *progressor.Progress) (chan map[string]interface{}, error) {
	query := self.makeQuery(p)
	fmt.Println(query)

	ret := make(chan map[string]interface{})
	go func() {
		defer close(ret)
		founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
		if err != nil {
			fmt.Println(`MapContext`, err.Error())
			return
		}
		for _, found := range founds {
			ret <- found
		}
	}()

	return ret, nil
}
