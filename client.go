package goreplica

import (
	"encoding/gob"
	"log"
	"net"
	"sync"
	"time"
)

type ReplicationClient struct {
	addr     string
	keys     map[string]int64
	kLock    sync.RWMutex
	deadLine int64
}

func NewReplicationClient(addr string) *ReplicationClient {
	var rc ReplicationClient
	rc.addr = addr
	rc.keys = make(map[string]int64)
	return &rc
}

func (rc *ReplicationClient) SetDeadline(dl int64) {
	rc.deadLine = dl
}

func (rc *ReplicationClient) dropAllKeys() {
	rc.keys = make(map[string]int64)
}

func (rc *ReplicationClient) DropAllKeys() {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	rc.dropAllKeys()
}

func (rc *ReplicationClient) SetKeys(k map[string]int64) {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	rc.dropAllKeys()
	for key, ver := range k {
		rc.keys[key] = ver
	}
}

func (rc *ReplicationClient) AddKey(k string, version int64) {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	rc.keys[k] = version
}

func (rc *ReplicationClient) DeleteKey(k string) {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	delete(rc.keys, k)
}

func (rc *ReplicationClient) HasKey(k string) bool {
	rc.kLock.RLock()
	defer rc.kLock.RUnlock()
	_, ok := rc.keys[k]
	return ok
}

func (rc *ReplicationClient) GetKeys() map[string]int64 {
	rc.kLock.RLock()
	defer rc.kLock.RUnlock()
	k := make(map[string]int64)
	for key, val := range rc.keys {
		k[key] = val
	}
	return k
}

func (rc *ReplicationClient) ReplicationGetAll() (ContentWatcher, error) {
	keys := make(map[string]int64)
	keys[READ_ALL] = 0

	return rc.replicationGetKeys(keys)
}

func (rc *ReplicationClient) ReplicationGetKeys() (ContentWatcher, error) {
	keys := rc.GetKeys()
	if len(keys) < 1 {
		return rc.ReplicationGetAll()
	}
	return rc.replicationGetKeys(keys)
}

func (rc *ReplicationClient) replicationGetKeys(keys map[string]int64) (ContentWatcher, error) {
	conn, err := net.Dial("tcp", rc.addr)
	if err != nil {
		return ContentWatcher{}, err
	}

	if rc.deadLine > 0 {
		_ = conn.SetDeadline(time.Now().Add(time.Duration(rc.deadLine) * time.Second))
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			log.Printf("Error while closing connection: %s\n", err)
		}
	}()

	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(keys)

	if err != nil {
		log.Printf("Error while send command to server: %s\n", err)
	}

	decoder := gob.NewDecoder(conn)
	var cw ContentWatcher
	err = decoder.Decode(&cw)

	if err != nil {
		return ContentWatcher{}, err
	}

	return cw, nil
}
