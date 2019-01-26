package orm

import "sync"

var (
	dbCache = &databaseCache{cache: make(map[string]*database)}
)

// database alias cacher.
type databaseCache struct {
	mux   sync.RWMutex
	cache map[string]*database
}

// add database with original name.
func (dc *databaseCache) add(name string, db *database) (added bool) {
	dc.mux.Lock()
	defer dc.mux.Unlock()
	if _, ok := dc.cache[name]; !ok {
		dc.cache[name] = db
		return true
	}
	return false
}

// get database if cached.
func (dc *databaseCache) get(name string) (db *database, ok bool) {
	dc.mux.RLock()
	defer dc.mux.RUnlock()
	db, ok = dc.cache[name]
	return
}

// get default database.
func (dc *databaseCache) getDefault() (db *database) {
	db, _ = dc.get("default")
	return
}
