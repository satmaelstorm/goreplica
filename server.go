package goreplica

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicationHook func(string, int64, ContentItem) ContentItem

type ReplicationServer struct {
	cw           ContentWatcher
	cwHooks      map[string]ReplicationHook
	cwlock       sync.RWMutex
	listener     net.Listener
	addr         string
	stop         chan bool
	connections  int
	cLock        sync.RWMutex
	stopComplete atomic.Value
	debugLogger  *log.Logger
	errorLogger  *log.Logger
	run          atomic.Value
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
	server.cwHooks = make(map[string]ReplicationHook)
	server.debugLogger = log.New(os.Stdout, "[GOREPLICA SERVER DEBUG] ", log.LstdFlags)
	server.errorLogger = log.New(os.Stderr, "[GOREPLICA SERVER ERROR] ", log.LstdFlags)
	server.run.Store(false)

	return &server, nil
}

func (rs *ReplicationServer) Serve() {
	go func(rs *ReplicationServer) {
		rs.debugLogger.Println("Replication Server started...")
		rs.run.Store(true)
		go func(rs *ReplicationServer) {
			<- rs.stop
			rs.run.Store(true)
			rs.listener.Close()
			rs.stopComplete.Store(true)
			rs.debugLogger.Println("Replication Server shutdown...")
		}(rs)
		for rs.run.Load().(bool) {
			conn, err := rs.listener.Accept()
			if err != nil {
				e := fmt.Sprintf("%s", err.Error())
				if strings.Contains(e, "use of closed network connection") {
					rs.debugLogger.Println(e)
					break //уже закрыт коннекшн
				}
				rs.errorLogger.Println(e)
				continue
			}
			rs.cLock.Lock()
			rs.connections++
			rs.cLock.Unlock()
			err = conn.SetReadDeadline(time.Now().Add(time.Duration(3 * time.Second)))
			if err != nil {
				rs.errorLogger.Println(err.Error())
				continue
			}
			go rs.handleConn(conn)
		}
	}(rs)
}

//func (rs *ReplicationServer) oldServe() {
//	go func(rs *ReplicationServer) {
//		rs.debugLogger.Println("Replication Server started...")
//		rs.run = true
//		hasFreeAccept := false
//		var mu sync.Mutex
//		for rs.run {
//			mu.Lock()
//			select {
//			case <-rs.stop:
//				rs.run = false
//				close(rs.stop)
//				rs.listener.Close()
//				rs.stopComplete.Store(true)
//				rs.debugLogger.Println("Replication Server shutdown...")
//			default:
//				if !hasFreeAccept && rs.run {
//					hasFreeAccept = true
//					go func(hasFreeAccept *bool) {
//						conn, err := rs.listener.Accept()
//						*hasFreeAccept = false
//						if err != nil {
//							rs.errorLogger.Println(err.Error())
//							return
//						}
//						rs.cLock.Lock()
//						rs.connections++
//						rs.cLock.Unlock()
//						err = conn.SetReadDeadline(time.Now().Add(time.Duration(3 * time.Second)))
//						if err != nil {
//							rs.errorLogger.Println(err.Error())
//							return
//						}
//						go rs.handleConn(conn)
//					}(&hasFreeAccept)
//				}
//			}
//			mu.Unlock()
//		}
//	}(rs)
//}

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
		rs.errorLogger.Printf("Error while read command: %s\n", err)
		return
	}

	encoder := gob.NewEncoder(conn)
	_, ok := keys[READ_ALL]
	if len(keys) == 1 && ok {
		err := encoder.Encode(rs.cw)
		if err != nil {
			rs.errorLogger.Println(err.Error())
		}
		return
	}

	ncw := NewContentWatcher()

	for key, version := range keys {
		if rs.cw.IsSet(key) {
			hook, ok := rs.cwHooks[key]
			if ok {
				ci := hook(key, version, rs.cw.Vars[key])
				if ci.Version > version || 0 == version {
					ncw.Vars[key] = ci
				}
			} else {
				v, _ := rs.cw.Get(key)
				ver, _ := rs.cw.GetVersion(key)
				if ver > version || 0 == version {
					ncw.Set(key, ver, v)
				}
			}
		}
	}

	err = encoder.Encode(ncw)
	if err != nil {
		rs.errorLogger.Println(err.Error())
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

func (rs *ReplicationServer) SetHook(key string, hook ReplicationHook) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cwHooks[key] = hook
}

func (rs *ReplicationServer) Unset(key string) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	rs.cw.Unset(key)
}

func (rs *ReplicationServer) UnsetHook(key string) {
	rs.cwlock.Lock()
	defer rs.cwlock.Unlock()
	delete(rs.cwHooks, key)
}

func (rs *ReplicationServer) IsSet(key string) bool {
	rs.cwlock.RLock()
	defer rs.cwlock.RUnlock()
	return rs.cw.IsSet(key)
}

func (rs *ReplicationServer) IsSetHook(key string) bool {
	rs.cwlock.RLock()
	defer rs.cwlock.RUnlock()
	_, ok := rs.cwHooks[key]
	return ok
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

func (rs *ReplicationServer) SetDebugLogger(l *log.Logger) {
	rs.debugLogger = l
}

func (rs *ReplicationServer) SetErrorLogger(l *log.Logger) {
	rs.errorLogger = l
}
