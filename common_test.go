package goreplica

import (
	"encoding/gob"
	"sync"
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

func TestReplicationClientOneKey(t *testing.T) {
	rs, err := NewReplicationServer("localhost:8086")
	if err != nil {
		t.Error(err.Error())
		return
	}
	rs.Serve()

	rs.Set("int", 1)
	rs.Set("str", "string")
	rs.Set("bool", true)

	rc := NewReplicationClient("localhost:8086")

	result, _ := rc.ReplicationGet()

	i, ok := result.Get("int")
	if !ok {
		t.Errorf("No item with key \"int\" in ContentWatcher")
	} else if i.(int) != 1 {
		t.Errorf("Items is not same!")
	}

	i, ok = result.Get("str")
	if !ok {
		t.Errorf("No item with key \"str\" in ContentWatcher")
	} else if i.(string) != "string" {
		t.Errorf("Items is not same!")
	}

	i, ok = result.Get("bool")
	if !ok {
		t.Errorf("No item with key \"bool\" in ContentWatcher")
	} else if i.(bool) != true {
		t.Errorf("Items is not same!")
	}

	var keys []string
	keys = append(keys, "int")
	keys = append(keys, "bool")

	rs.Set("int", 2)

	result, _ = rc.ReplicationGetKeys(keys)

	i, ok = result.Get("int")
	if !ok {
		t.Errorf("No item with key \"int\" in ContentWatcher")
	} else if i.(int) !=2 {
		t.Errorf("Items is not same!")
	}

	i, ok = result.Get("str")
	if ok {
		t.Errorf("Item with key \"str\" in ContentWatcher!")
	}

	i, ok = result.Get("bool")
	if !ok {
		t.Errorf("No item with key \"bool\" in ContentWatcher")
	} else if i.(bool) != true {
		t.Errorf("Items is not same!")
	}
	rs.GracefulStop()
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

var o sync.Once;

func BenchmarkReplicationClient_ReplicationGet(b *testing.B) {
	var item testStruct
	item.A = 1
	item.B = "testing"
	item.C = make(map[string]bool)
	item.C["1"] = true
	item.C["2"] = true
	item.C["3"] = true

	o.Do(func() {
		gob.Register(testStruct{})
		rs, err := NewReplicationServer("localhost:8086")
		if nil == err {
			rs.Serve()
			rs.Set("item", item)
		}
	})

	b.RunParallel(func (pb *testing.PB){
		for pb.Next() {
			rc := NewReplicationClient("localhost:8086")
			result, err := rc.ReplicationGet()

			if err != nil {
				b.Error(err.Error())
				return
			}

			item2, ok := result.Get("item")

			if !ok {
				b.Errorf("No item key in ContentWatcher")
				return
			}
			item3 := item2.(testStruct)

			if item3.A != item.A || item3.B != item.B || item3.C["1"] != item.C["1"] {
				b.Errorf("Items is not same!")
			}
		}
	})
}
