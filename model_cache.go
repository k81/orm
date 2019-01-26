// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
