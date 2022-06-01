package mssql

import (
	"sync"

	"github.com/ws6/calculator/utils/dbhelper"
	"github.com/ws6/msi"
)

var AddConnectionPool, GetConnectionPoolByName, CloseConnectionPool = func() (
	func(string, *msi.Msi),
	func(string) *msi.Msi,
	func() error,
) {
	cache := make(map[string]*msi.Msi)
	var lock sync.Mutex
	return func(s string, db *msi.Msi) {
			lock.Lock()
			cache[s] = db
			lock.Unlock()
		}, func(s string) *msi.Msi {
			lock.Lock()
			found, ok := cache[s]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}, func() error {
			lock.Lock()
			defer lock.Unlock()
			for _, db := range cache {
				if err := db.Close(); err != nil {
					return err
				}
			}
			return nil
		}
}()

func createIfNotExistDb(key string, cfg map[string]string) (*msi.Msi, error) {
	ret := GetConnectionPoolByName(key)
	if ret != nil {
		return ret, nil
	}
	db, err := dbhelper.GetMSDB(cfg)
	if err != nil {
		return nil, err
	}
	AddConnectionPool(key, db)
	return db, nil
}
