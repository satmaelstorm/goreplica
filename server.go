package goreplica

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
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
		log.Printf("[%d] Replication Server started...", os.Getpid())
		canAccept := make(chan bool, 1)
		canAccept <- true
		var mu sync.Mutex
		for {
			mu.Lock()
			select {
			case <-rs.stop:
				log.Println("Replication Server shutdown...")
				close(canAccept)
				close(rs.stop)
				rs.listener.Close()
				break
			case x:= <- canAccept:
				if x {
					go func(ch chan bool) {
						conn, err := rs.listener.Accept()
						ch <- true
						if err != nil {
							log.Println(err.Error())
							return
						}
						rs.cLock.Lock()
						rs.connections++
						rs.cLock.Unlock()
						go rs.handleConn(conn)
					}(canAccept)
				}
			default:

			}
			mu.Unlock()
		}
	}(rs)
}

func (rs *ReplicationServer) handleConn(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Printf("Error while colsing connection: %s", err)
		}
	}()
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
