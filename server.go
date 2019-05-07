package goreplica

import (
	"encoding/gob"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicationServer struct {
	cw           ContentWatcher
	cwlock       sync.RWMutex
	listener     net.Listener
	addr         string
	stop         chan bool
	connections  int
	cLock        sync.RWMutex
	stopComplete atomic.Value
}

func NewReplicationServer(addr string) (*ReplicationServer, error) {
	var server ReplicationServer
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return &ReplicationServer{}, err
	}

	server.cw = NewContentWatcher()
	server.listener = listener
	server.addr = addr
	server.stop = make(chan bool)
	server.connections = 0
	server.stopComplete.Store(false)

	return &server, nil
}

func (rs *ReplicationServer) Serve() {
	go func(rs *ReplicationServer) {
		log.Println("Replication Server started...")
		run := true
		hasFreeAccept := false
		var mu sync.Mutex
		for run {
			mu.Lock()
			select {
			case <-rs.stop:
				run = false
				close(rs.stop)
				rs.listener.Close()
				rs.stopComplete.Store(true)
				log.Println("Replication Server shutdown...")
			default:
				if !hasFreeAccept && run {
					hasFreeAccept = true
					go func(hasFreeAccept *bool) {
						conn, err := rs.listener.Accept()
						*hasFreeAccept = false
						if err != nil {
							log.Println(err.Error())
							return
						}
						rs.cLock.Lock()
						rs.connections++
						rs.cLock.Unlock()
						err = conn.SetReadDeadline(time.Now().Add(time.Duration(3 * time.Second)))
						if err != nil {
							log.Println(err.Error())
							return
						}
						go rs.handleConn(conn)
					}(&hasFreeAccept)
				}
			}
			mu.Unlock()
		}
	}(rs)
}

func (rs *ReplicationServer) handleConn(conn net.Conn) {
	defer func() {
		rs.cLock.Lock()
		rs.connections--
		rs.cLock.Unlock()
	}()

	defer conn.Close()
	rs.cwlock.RLock()
	defer rs.cwlock.RUnlock()
	keys := make(map[string]int64)

	decoder := gob.NewDecoder(conn)
	err := decoder.Decode(&keys)
	if err != nil {
		log.Printf("Error while read command: %s\n", err)
		return
	}

	encoder := gob.NewEncoder(conn)
	_, ok := keys[READ_ALL]
	if len(keys) == 1 && ok {
		err := encoder.Encode(rs.cw)
		if err != nil {
			log.Println(err.Error())
		}
		return
	}

	ncw := NewContentWatcher()

	for key, version := range keys {
		if rs.cw.IsSet(key) {
			v, _ := rs.cw.Get(key)
			ver, _ := rs.cw.GetVersion(key)
			if ver >= version {
				ncw.Set(key, ver, v)
			}
		}
	}

	err = encoder.Encode(ncw)
	if err != nil {
		log.Println(err.Error())
	}
}

func (rs *ReplicationServer) Stop() {
	rs.stop <- true
}

func (rs *ReplicationServer) GetConnections() int {
	rs.cLock.RLock()
	defer rs.cLock.RUnlock()
	return rs.connections
}

func (rs *ReplicationServer) Set(key string, version int64, val interface{}) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cw.Set(key, version, val)
}

func (rs *ReplicationServer) Unset(key string) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cw.Unset(key)
}

func (rs ReplicationServer) IsSet(key string) bool {
	rs.cwlock.RLock()
	defer rs.cwlock.RUnlock()
	return rs.cw.IsSet(key)
}

func (rs *ReplicationServer) GracefulStop() {
	rs.Stop()
	for !rs.stopComplete.Load().(bool) {
		time.Sleep(1)
	}
	for rs.GetConnections() > 0 {
		time.Sleep(1)
	}
}

func (rs *ReplicationServer) RegisterType(value interface{}) {
	gob.Register(value)
}
