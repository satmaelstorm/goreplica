package goreplica

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type ReplicationServer struct {
	cw          ContentWatcher
	cwlock      sync.RWMutex
	listener    net.Listener
	addr        string
	stop        chan bool
	connections int
	cLock       sync.RWMutex
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
				log.Println("Replication Server shutdown...")
				run = false
				close(rs.stop)
				rs.listener.Close()
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
						go rs.handleConn(conn)
					}(&hasFreeAccept)
				}
			}
			mu.Unlock()
		}
	}(rs)
}

func (rs *ReplicationServer) handleConn(conn net.Conn) {
	defer conn.Close()
	rs.cwlock.RLock()
	defer rs.cwlock.RUnlock()
	encoder := gob.NewEncoder(conn)
	err := encoder.Encode(rs.cw)
	if err != nil {
		fmt.Println(err.Error())
	}
	rs.cLock.Lock()
	rs.connections--
	rs.cLock.Unlock()
}

func (rs *ReplicationServer) Stop() {
	rs.stop <- true
}

func (rs *ReplicationServer) GetConnections() int {
	rs.cLock.RLock()
	defer rs.cLock.RUnlock()
	return rs.connections
}

func (rs *ReplicationServer) Set(key string, val interface{}) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cw.Set(key, val)
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
	for rs.GetConnections() > 0 {
		time.Sleep(1)
	}
}

func (rs *ReplicationServer) RegisterType(value interface{}) {
	gob.Register(value)
}
