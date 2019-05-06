package goreplica

import (
	"encoding/gob"
	"log"
	"net"
)

type ReplicationClient struct {
	addr string
}

func NewReplicationClient(addr string) ReplicationClient {
	var rc ReplicationClient
	rc.addr = addr
	return rc
}

func (rc ReplicationClient) ReplicationGet() (ContentWatcher, error) {
	var keys []string
	keys = append(keys, READ_ALL)

	return rc.ReplicationGetKeys(keys)
}

func (rc ReplicationClient) ReplicationGetKeys(keys []string) (ContentWatcher, error) {
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
