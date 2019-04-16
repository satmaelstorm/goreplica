package goreplica

import (
	"encoding/gob"
	"testing"
)

type testStruct struct {
	A int
	B string
	C map[string]bool
}

func TestContentWatcher_Add(t *testing.T) {
	var item testStruct
	item.A = 1
	item.B = "testing"
	item.C = make(map[string]bool)
	item.C["1"] = true

	cw := NewContentWatcher()
	cw.Set("item", item)

	item.A = 2
	item.B = "test3"
	item.C["1"] = false

	cw.Set("item", item)

	item2, ok := cw.Get("item")

	if !ok {
		t.Errorf("No item key in ContentWatcher")
		return
	}

    item3 := item2.(testStruct)

	if item3.A != item.A || item3.B != item.B || item3.C["1"] != item.C["1"]{
		t.Errorf("Items is not same!")
	}
}

func TestReplicationServer(t *testing.T) {
	gob.Register(testStruct{})
	rs, err := NewReplicationServer("localhost:8086")
	if err != nil {
		t.Error(err.Error())
		return
	}
	rs.Serve()

	var item testStruct
	item.A = 1
	item.B = "testing"
	item.C = make(map[string]bool)
	item.C["1"] = true

	rs.Set("item", item)

	rc := NewReplicationClient("localhost:8086")

	result, err := rc.ReplicationGet()

	if err != nil {
		rs.Stop()
		t.Error(err.Error())
		return
	}

	item2, ok := result.Get("item")

	if !ok {
		t.Errorf("No item key in ContentWatcher")
		return
	}
	item3 := item2.(testStruct)

	if item3.A != item.A || item3.B != item.B || item3.C["1"] != item.C["1"]{
		t.Errorf("Items is not same!")
	}

	item.A = 2
	item.B = "test2"
	item.C["1"] = false

	rs.Set("item", item)

	result, err = rc.ReplicationGet()

	if err != nil {
		rs.Stop()
		t.Error(err.Error())
		return
	}

	item4, ok := result.Get("item")

	item5 := item4.(testStruct)

	if item5.A != item.A || item5.B != item.B || item5.C["1"] != item.C["1"]{
		t.Errorf("Items is not same!")
	}

	rs.Stop()
}
