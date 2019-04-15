package goreplica

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
)

type ReplicationServer struct {
	cw          ContentWatcher
	cwlock		sync.Mutex
	listener    net.Listener
	addr        string
	stop        chan bool
	connections int
}

func NewReplicationServer(addr string) (ReplicationServer, error) {
	var server ReplicationServer
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return ReplicationServer{}, err
	}

	server.cw = NewContentWatcher()
	server.listener = listener
	server.addr = addr
	server.stop = make(chan bool)
	server.connections = 0

	return server, nil
}

func (rs ReplicationServer) Serve() {
	go func(rs ReplicationServer) {
		log.Println("Replication Server started...")
		run := true
		hasFreeAccept := false
		for run {
			select {
			case <-rs.stop: //это пока не работает
				log.Println("Replication Server shutdown...")
				close(rs.stop)
				run = false
				rs.listener.Close()
			default:
				go func(hasFreeAccept *bool, run bool) {
					if *hasFreeAccept || !run {
						return //не надо плодить много горутин с приемом
					}
					*hasFreeAccept = true
					log.Println("New Replication Accept started")
					conn, err := rs.listener.Accept()
					*hasFreeAccept = false
					if err != nil {
						log.Println(err.Error())
						return
					}
					rs.connections++
					go rs.handleConn(conn)
				}(&hasFreeAccept, run)
			}
		}
	}(rs)
}

func (rs ReplicationServer) handleConn(conn net.Conn) {
	defer conn.Close()
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	encoder := gob.NewEncoder(conn)
	err := encoder.Encode(rs.cw)
	if err != nil {
		fmt.Println(err.Error())
	}
	rs.connections--
}

func (rs ReplicationServer) Stop() {
	rs.stop <- true
}

func (rs ReplicationServer) GetConnections() int {
	return rs.connections
}

func (rs ReplicationServer) Set (key string, val interface{}) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cw.Add(key, val)
}

func (rs ReplicationServer) Unset (key string) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cw.Delete(key)
}

func (rs ReplicationServer) IsSet (key string) bool {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	return rs.cw.Has(key)
}
