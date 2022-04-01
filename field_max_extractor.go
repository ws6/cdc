package cdc

//field_max_extractor.go to pull the max value on cron schedule

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/msi"
)

func init() {
	extraction.RegisterType(new(FieldMaxExtractor))
}

type FieldMaxExtractor struct {
	fields []*TableField
	db     *msi.Msi
	cfg    *confighelper.SectionConfig
	progr  progressor.Progressor
}

func (self *FieldMaxExtractor) Close() error {

	if self.db != nil {
		return self.db.Close()
	}
	self.db = nil //force GC
	fmt.Println(`FieldMaxExtractor Closed`)
	return nil
}
func (self *FieldMaxExtractor) Type() string {
	return `FieldMaxExtractor`
}

func (self *FieldMaxExtractor) Name() string {
	//TODO from config
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()
}

func (self *FieldMaxExtractor) SaveProgressOnFail(error) bool {
	return false
}

func (self *FieldMaxExtractor) UpdateProgress(item map[string]interface{}, p *progressor.Progress) error {
	p.Timestamp = time.Now().Local()
	return nil
}

func (class *FieldMaxExtractor) NewIncref(cfg *confighelper.SectionConfig) (extraction.Incref, error) {
	ret := new(FieldMaxExtractor)
	ret.cfg = cfg
	//TODO open database
	var err error
	key := cfg.SectionName

	ret.db, err = createIfNotExistDb(key, cfg.ConfigMap)
	if err != nil {
		return nil, err
	}
	ret.fields, err = ret.getFields()
	if err != nil {
		return nil, err
	}

	return ret, nil
}
func (self *FieldMaxExtractor) getFields() ([]*TableField, error) {
	ret := []*TableField{}
	fieldStr := self.cfg.ConfigMap[`to_scan_fields`]
	fieldStr = strings.TrimSpace(fieldStr)
	if fieldStr == "" {
		return nil, fmt.Errorf(`need to_scan_fields`)
	}
	sp := strings.Split(fieldStr, ";")
	if len(sp) == 0 {
		return nil, fmt.Errorf(`need to_scan_fields value`)
	}

	for _, f := range sp {
		sp2 := strings.Split(f, ".")
		if len(sp2) != 3 {
			return nil, fmt.Errorf(`wrong format. expect $schema.$tableName.$fieldName`)
		}
		topush := new(TableField)
		ret = append(ret, topush)
		topush.SchameName = sp2[0]
		topush.TableName = sp2[1]
		topush.ColumnName = sp2[2]
	}

	return ret, nil
}

func (self *FieldMaxExtractor) getMax(ctx context.Context, f *TableField) (string, error) {
	query := fmt.Sprintf(`
	select 
max([%s]) as max_value
from [%s].[%s]
	`,
		f.ColumnName,
		f.SchameName,
		f.TableName,
	)
	founds, err := self.db.MapContext(ctx, self.db.Db, query, nil)
	if err != nil {
		return "", err
	}
	if len(founds) == 0 {
		return "", msi.NOT_FOUND
	}

	return fmt.Sprintf("%v", founds[0][`max_value`]), nil
}

func (self *FieldMaxExtractor) GetChan(ctx context.Context, p *progressor.Progress) (chan map[string]interface{}, error) {

	ret := make(chan map[string]interface{})
	go func() {
		defer close(ret)
		for _, f := range self.fields {
			maxVal, err := self.getMax(ctx, f)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			if maxVal == f.MaxValue {
				//prevent excessive messages
				continue
			}
			f.MaxValue = maxVal
			body, _ := json.Marshal(f)
			topub := map[string]interface{}{}
			json.Unmarshal(body, &topub)

			select {
			case <-ctx.Done():
				return
			case ret <- topub:
			}

		}

	}()

	return ret, nil
}
