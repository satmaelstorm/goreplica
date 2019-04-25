package goreplica

import (
	"encoding/gob"
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
	conn, err := net.Dial("tcp", rc.addr)
	if err != nil {
		return ContentWatcher{}, err
	}
	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	var cw ContentWatcher
	err = decoder.Decode(&cw)

	if err != nil {
		return ContentWatcher{}, err
	}

	return cw, nil
}
