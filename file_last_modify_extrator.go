package cdc

//file_last_modify_extractor.go detect given list of directories one-level down last-modified

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/msi"
)

func init() {
	extraction.RegisterType(new(FileLastMod))
}

type FileLastMod struct {
	watchingDirs []string
	cfg          *confighelper.SectionConfig
}

func (self *FileLastMod) Type() string {
	return `FileLastMod`
}
func (self *FileLastMod) Name() string {
	//TODO from config
	site, err := self.cfg.Configer.String(
		fmt.Sprintf(`%s::site`, self.cfg.SectionName),
	)
	if err == nil {
		return fmt.Sprintf(`%s.%s`, self.Type(), site)
	}
	return self.Type()

}

func (self *FileLastMod) Close() error {
	return nil
}

func (self *FileLastMod) IsNewer(f fs.FileInfo, p *progressor.Progress) bool {
	if p.Timestamp.IsZero() {
		return true
	}
	return p.Timestamp.Before(f.ModTime())
}

func (self *FileLastMod) toMsi(f fs.FileInfo) map[string]interface{} {

	ret := map[string]interface{}{
		`IsDir`:       f.IsDir(),
		`Mode`:        f.Mode(),
		`ModTime`:     f.ModTime(),
		`modify_date`: f.ModTime(),
		`Name`:        f.Name(),
		`Size`:        f.Size(),
	}
	return ret
}

// {
//   "IsDir": false,
//   "ModTime": "2020-11-30T12:21:32.4280474-08:00",
//   "Mode": 438,
//   "Name": "O000005_KB_13Nov2020.xlsm",
//   "Size": 559306,
//   "abs_path": "\\\\usfc-prd-isi01\\CGLS_Onco\\QF-0345\\TSO\\O000005_KB_13Nov2020.xlsm",
//   "modify_date": "2020-11-30T12:21:32.4280474-08:00"
// }

func (self *FileLastMod) GetChan(ctx context.Context, _p *progressor.Progress) (chan map[string]interface{}, error) {
	p := new(progressor.Progress)
	_p.Copy(p)
	fmt.Println(`using progress`, p)
	ret := make(chan map[string]interface{})
	go func() {
		defer close(ret)
		for _, _dir := range self.watchingDirs {
			absDir, err := filepath.Abs(_dir)
			if err != nil {
				//TODO go producer DLQ?
				fmt.Println(err.Error())
				continue
			}
			dirInfo, err := os.Stat(absDir)
			if err != nil {
				//TODO go producer DLQ?
				fmt.Println(err.Error())
				continue
			}
			if !dirInfo.IsDir() {
				fmt.Println(`not a directory`, absDir)
				continue
			}

			files, err := ioutil.ReadDir(absDir)
			if err != nil {
				//TODO go producer DLQ?
				fmt.Println(err.Error())
				continue
			}

			for _, f := range files {
				if !self.IsNewer(f, p) {
					continue
				}

				topub := self.toMsi(f)
				topub[`abs_path`] = filepath.Join(absDir, f.Name())
				select {
				case <-ctx.Done():
					return
				case ret <- topub:

				}
			}

		}

	}()
	return ret, nil
}

func (class *FileLastMod) NewIncref(cfg *confighelper.SectionConfig) (extraction.Incref, error) {
	ret := new(FileLastMod)
	ret.cfg = cfg
	dirs, err := ret.cfg.Configer.Strings(
		fmt.Sprintf(`%s::watching_directories`, ret.cfg.SectionName),
	)
	if err != nil {
		return nil, err
	}
	ret.watchingDirs = dirs
	return ret, nil
}

func (self *FileLastMod) SaveProgressOnFail(error) bool {
	return false
}
func (self *FileLastMod) UpdateProgress(item map[string]interface{}, p *progressor.Progress) error {

	modify_date, err := msi.ToTime(item[`modify_date`])
	if err != nil {
		fmt.Println(item)
		fmt.Println(`modify_date`, err.Error())
		return nil
	}
	if modify_date != nil {
		if modify_date.After(p.Timestamp) {
			p.Timestamp = *modify_date
		}

	}
	return nil
}
