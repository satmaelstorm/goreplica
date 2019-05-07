package goreplica

import "encoding/gob"

const READ_ALL = "__(all_keys)__"

func init() {
	gob.Register(ContentWatcher{})
}

type ContentWatcher struct {
	Vars map[string]contentItem
}

type contentItem struct {
	Val     interface{}
	Version int64
}

func (cw *ContentWatcher) Set(key string, version int64, val interface{}) {
	cw.Vars[key] = contentItem{Val: val, Version: version}
}

func (cw *ContentWatcher) Unset(key string) {
	delete(cw.Vars, key)
}

func (cw *ContentWatcher) Get(key string) (interface{}, bool) {
	r, ok := cw.Vars[key]
	if ok {
		return r.Val, ok
	}
	return nil, ok
}

func (cw *ContentWatcher) GetVersion(key string) (int64, bool) {
	r, ok := cw.Vars[key]
	if ok {
		return r.Version, ok
	}
	return 0, ok
}

func (cw *ContentWatcher) IsSet(key string) bool {
	_, ok := cw.Vars[key]
	return ok
}

func NewContentWatcher() ContentWatcher {
	var cw ContentWatcher
	cw.Vars = make(map[string]contentItem)
	return cw
}
