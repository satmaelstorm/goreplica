package goreplica

import "encoding/gob"

func init() {
	gob.Register(ContentWatcher{})
}

type ContentWatcher struct {
	Vars map[string]interface{}
}

func (cw *ContentWatcher) Add (key string, val interface{}) {
	cw.Vars[key] = val
}

func (cw *ContentWatcher) Delete (key string) {
	delete(cw.Vars, key)
}

func (cw *ContentWatcher) Get (key string) (interface{}, bool) {
	r, ok := cw.Vars[key]
	return r, ok
}

func (cw *ContentWatcher) Has (key string) bool {
	_, ok := cw.Vars[key]
	return ok
}

func NewContentWatcher() ContentWatcher {
	var cw ContentWatcher
	cw.Vars = make(map[string]interface{})
	return cw
}

