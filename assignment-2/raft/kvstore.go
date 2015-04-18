package raft

import (
	"container/heap"
	"fmt"
	handler "github.com/saurabh-hote/cs733/assignment-2/handler"
	"log"
	"time"
)

type command handler.Command

type value struct {
	data     []byte
	numbytes int
	version  uint64
	expiry   int64
}

//Map Manager
func InitializeKVStore(ch chan LogEntry) { //	This channel has to be of type MyLogEntry. This is commitCh
	//The map which actually stores values
	m := make(map[string]value)
	h := &nodeHeap{}
	var counter uint64 = 0
	const heapCleanupInterval = 60
	go cleaner(heapCleanupInterval, ch)
	for {
		logEntry := <-ch
		cmd, _ := handler.DecodeCommand(logEntry.Data())
		r := "ERR_NOT_FOUND\r\n"
		val, ok := m[cmd.Key]
		switch cmd.Action {
		case handler.Set:
			{
				version := counter
				counter++
				t := cmd.Expiry
				if t != 0 {
					t += time.Now().Unix()
				}
				m[cmd.Key] = value{cmd.Data, cmd.Numbytes, version, t}
				if cmd.Expiry != 0 {
					heap.Push(h, node{t, cmd.Key, version})
				}
				r = fmt.Sprintf("OK %v\r\n", version)
			}
		case handler.Get:
			{
				if ok {
					r = fmt.Sprintf("VALUE %v\r\n"+string(val.data)+"\r\n", val.numbytes)
				}
			}
		case handler.Getm:
			{
				if ok {
					t := val.expiry
					if t != 0 {
						t = val.expiry - time.Now().Unix() // remaining time
					}
					r = fmt.Sprintf("VALUE %v %v %v\r\n"+string(val.data)+"\r\n", val.version, t, val.numbytes)
				}
			}
		case handler.Cas:
			{
				if ok {
					if val.version == cmd.Version {
						t := cmd.Expiry
						if t != 0 {
							t += time.Now().Unix()
						}
						version := counter
						counter++
						m[cmd.Key] = value{cmd.Data, cmd.Numbytes, version, t}
						if cmd.Expiry != 0 {
							heap.Push(h, node{t, cmd.Key, version})
						}
						r = fmt.Sprintf("OK %v\r\n", version)
					} else {
						r = fmt.Sprintf("ERR_VERSION\r\n")
					}
				}
			}
		case handler.Delete:
			{
				if ok {
					delete(m, cmd.Key)
					r = "DELETED\r\n"
				}
			}
		case handler.Cleanup:
			{
				t := time.Now().Unix()
				for (*h).Len() != 0 && (*h)[0].expiry <= t {
					root := heap.Pop(h).(node)
					v, e := m[root.key]
					if e && root.version == v.version {
						delete(m, root.key)
					}
				}
				r = "CLEANED\r\n"
			}
		default:
			{
				r = "ERR_INTERNAL\r\n"
			}
		}

		if cmd.Action != handler.Cleanup {
			// Send response to appropriate handler's channel
			ResponseChannelStore.RLock()
			responseChannel := ResponseChannelStore.m[logEntry.Lsn()]
			ResponseChannelStore.RUnlock()

			if responseChannel == nil {
				log.Println("KVStore: Response channel for LogEntry not found")
			} else {
				//Delete the entry for response channel handle
				ResponseChannelStore.Lock()
				delete(ResponseChannelStore.m, logEntry.Lsn())
				ResponseChannelStore.Unlock()
				*responseChannel <- r
			}
		}
	}
}

func cleaner(interval int, ch chan LogEntry) {
	command := handler.Command{handler.Cleanup, "", 0, 0, 0, nil}
	data, err := handler.EncodeCommand(command)
	logEntry := LogEntryObj{0, data, false}
	if err != nil {
		log.Println("Error encoding the command ", err.Error())
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for range ticker.C {
		ch <- logEntry
	}
}
