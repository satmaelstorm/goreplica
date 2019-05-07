package goreplica

import (
	"encoding/gob"
	"log"
	"net"
	"sync"
)

type ReplicationClient struct {
	addr  string
	keys  map[string]int64
	kLock sync.RWMutex
}

func NewReplicationClient(addr string) *ReplicationClient {
	var rc ReplicationClient
	rc.addr = addr
	rc.keys = make(map[string]int64)
	return &rc
}

func (rc *ReplicationClient) dropAllKeys() {
	rc.keys = make(map[string]int64)
}

func (rc *ReplicationClient) DropAllKeys() {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	rc.dropAllKeys()
}

func (rc *ReplicationClient) SetKeys(k []string) {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	rc.dropAllKeys()
	for _, key := range k {
		rc.keys[key] = 0
	}
}

func (rc *ReplicationClient) AddKey(k string) {
	rc.kLock.Lock()
	defer rc.kLock.Unlock()
	rc.keys[k] = 0
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

func (rc *ReplicationClient) GetKeys() []string {
	rc.kLock.RLock()
	defer rc.kLock.RUnlock()
	k := make([]string, len(rc.keys))
	i := 0
	for key, _ := range rc.keys {
		k[i] = key
		i++
	}
	return k
}

func (rc *ReplicationClient) ReplicationGetAll() (ContentWatcher, error) {
	var keys []string
	keys = append(keys, READ_ALL)

	return rc.replicationGetKeys(keys)
}

func (rc *ReplicationClient) ReplicationGetKeys() (ContentWatcher, error) {
	keys := rc.GetKeys()
	if len(keys) < 1 {
		return rc.ReplicationGetAll()
	}
	return rc.replicationGetKeys(keys)
}

func (rc *ReplicationClient) replicationGetKeys(keys []string) (ContentWatcher, error) {
	conn, err := net.Dial("tcp", rc.addr)
	if err != nil {
		return ContentWatcher{}, err
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
