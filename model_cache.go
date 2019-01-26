package orm

import (
	"sync"
)

const (
	defaultStructTagName  = "orm"
	defaultStructTagDelim = ";"
)

var (
	modelCache = &_modelCache{
		cache: make(map[string]*modelInfo),
	}
)

// model info collection
type _modelCache struct {
	sync.RWMutex // only used outsite for bootStrap
	cache        map[string]*modelInfo
	done         bool
}

// get model info by full name
func (mc *_modelCache) get(fullName string) (mi *modelInfo, ok bool) {
	mi, ok = mc.cache[fullName]
	return
}

// add model info to collection
func (mc *_modelCache) add(mi *modelInfo) *modelInfo {
	oldMi := mc.cache[mi.fullName]
	mc.cache[mi.fullName] = mi
	return oldMi
}

// clean all model info.
func (mc *_modelCache) clean() {
	mc.cache = make(map[string]*modelInfo)
	mc.done = false
}

// ResetModelCache Clean model cache. Then you can re-RegisterModel.
// Common use this api for test case.
func ResetModelCache() {
	modelCache.clean()
}
