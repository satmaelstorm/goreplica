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
	cw.Set("item", 0, item)

	item.A = 2
	item.B = "test3"
	item.C["1"] = false

	cw.Set("item", 0, item)

	item2, ok := cw.Get("item")

	if !ok {
		t.Errorf("No item key in ContentWatcher")
		return
	}

	item3 := item2.(testStruct)

	if item3.A != item.A || item3.B != item.B || item3.C["1"] != item.C["1"] {
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

	rs.Set("int", 0, 1)
	rs.Set("str", 0, "string")
	rs.Set("bool", 0, true)

	rc := NewReplicationClient("localhost:8086")

	result, _ := rc.ReplicationGetAll()

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

	keys := make(map[string]int64)
	keys["int"] = 0
	keys["bool"] = 0

	rs.Set("int", 1, 2)

	result, _ = rc.replicationGetKeys(keys)

	i, ok = result.Get("int")
	if !ok {
		t.Errorf("No item with key \"int\" in ContentWatcher")
	} else if i.(int) != 2 {
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

	rs.Set("item", 0, item)

	rc := NewReplicationClient("localhost:8086")

	result, err := rc.ReplicationGetAll()

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

	if item3.A != item.A || item3.B != item.B || item3.C["1"] != item.C["1"] {
		t.Errorf("Items is not same!")
	}

	item.A = 2
	item.B = "test2"
	item.C["1"] = false

	rs.Set("item", 1, item)

	result, err = rc.ReplicationGetAll()

	if err != nil {
		rs.Stop()
		t.Error(err.Error())
		return
	}

	item4, ok := result.Get("item")

	item5 := item4.(testStruct)

	if item5.A != item.A || item5.B != item.B || item5.C["1"] != item.C["1"] {
		t.Errorf("Items is not same!")
	}

	rs.GracefulStop()
}

func TestReplicationServerSetHook(t *testing.T) {
	rs, err := NewReplicationServer("localhost:8086")
	if err != nil {
		t.Error(err.Error())
		return
	}
	rs.Serve()

	rs.Set("int", 100, 1)
	rs.Set("str", 100, "string")
	rs.Set("bool", 100, true)

	rc := NewReplicationClient("localhost:8086")

	rc.AddKey("int", 101)
	rc.AddKey("bool", 99)

	cw, err := rc.ReplicationGetKeys()

	if err != nil {
		t.Errorf("Error: %s\n", err)
	} else {
		_, ok := cw.Get("int")
		b, okb := cw.Get("bool")
		if ok {
			t.Error("Key \"int\" is loaded")
		} else if !okb {
			t.Error("Key \"bool\" is not loaded")
		} else if b.(bool) != true {
			t.Error("Key \"bool\" is not true")
		}
	}
	rs.Set("hook", 100, 4)
	rs.SetHook("hook", func(s string, i int64, ci ContentItem) ContentItem {
		return ContentItem{Val:42, Version:i+1}
	})
	rc.AddKey("hook", 100)

	cw, err = rc.ReplicationGetKeys()
	if err != nil {
		t.Errorf("Error: %s\n", err)
	} else if !cw.IsSet("hook"){
		t.Errorf("Key \"hook\" is not loaded (%v)", cw)
	} else {
		val, _ := cw.Get("hook")
		ver, _ := cw.GetVersion("hook")
		if val.(int) != 42 || ver != 101 {
			t.Errorf("Key \"hook\" is invalid val: %v, ver: %d", val, ver)
		}
	}

	rs.GracefulStop()
}

func TestReplicationClientByKeys(t *testing.T) {
	rs, err := NewReplicationServer("localhost:8086")
	if err != nil {
		t.Error(err.Error())
		return
	}
	rs.Serve()

	rs.Set("int", 0, 1)
	rs.Set("str", 0, "string")
	rs.Set("bool", 0, true)

	rc := NewReplicationClient("localhost:8086")

	rc.AddKey("int", 0)
	cw, err := rc.ReplicationGetKeys()

	if err != nil {
		t.Errorf("Error: %s\n", err)
	} else {
		i, ok := cw.Get("int")
		if !ok {
			t.Error("Key \"int\" is not loaded")
		} else if i.(int) != 1 {
			t.Error("Key \"int\" is not 1")
		}
	}

	rc.DeleteKey("int")
	rc.AddKey("bool", 0)

	cw, err = rc.ReplicationGetKeys()

	if err != nil {
		t.Errorf("Error: %s\n", err)
	} else {
		_, ok := cw.Get("int")
		b, okb := cw.Get("bool")
		if ok {
			t.Error("Key \"int\" is loaded")
		} else if !okb {
			t.Error("Key \"bool\" is not loaded")
		} else if b.(bool) != true {
			t.Error("Key \"bool\" is not true")
		}
	}

	keys := make(map[string]int64)
	rc.SetKeys(keys)

	rs.Set("int", 1, 10)
	rs.Set("str", 1, "rdw")
	rs.Set("bool", 1, false)

	cw, err = rc.ReplicationGetKeys()

	if err != nil {
		t.Errorf("Error: %s\n", err)
	} else {
		i, oki := cw.Get("int")
		b, okb := cw.Get("bool")
		s, oks := cw.Get("str")
		if !oki || !okb || !oks {
			t.Errorf("Some keys is not loaded! (%v %v %v)\n", oki, okb, oks)
		} else if i.(int) != 10 || b.(bool) != false || s.(string) != "rdw" {
			t.Errorf("Some values is wrong! (%d %v %s)\n", i.(int), b.(bool), s.(string))
		}
	}

	rs.GracefulStop()
}

var o sync.Once

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
			rs.Set("item", 0, item)
		}
	})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rc := NewReplicationClient("localhost:8086")
			result, err := rc.ReplicationGetAll()

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
