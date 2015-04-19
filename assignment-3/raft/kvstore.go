package raft

import (
	"container/heap"
	"fmt"
	handler "github.com/saurabh-hote/cs733/assignment-3/handler"
	util "github.com/saurabh-hote/cs733/assignment-3/util"
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
func InitializeKVStore(serverID int, ch chan util.LogEntry) { //	This channel has to be of type MyLogEntry. This is commitCh
	//The map which actually stores values
	m := make(map[string]value)
	h := &nodeHeap{}
	var counter uint64 = 0
	const heapCleanupInterval = 60
	go cleaner(heapCleanupInterval, ch)
	for {
		logEntry := <-ch
		cmd, _ := handler.DecodeCommand(logEntry.Data())

		responseMsg := "ERR_NOT_FOUND\r\n"
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
				responseMsg = fmt.Sprintf("OK %v\r\n", version)
			}
		case handler.Get:
			{
				if ok {
					responseMsg = fmt.Sprintf("VALUE %v\r\n"+string(val.data)+"\r\n", val.numbytes)
				}
			}
		case handler.Getm:
			{
				if ok {
					t := val.expiry
					if t != 0 {
						t = val.expiry - time.Now().Unix() // remaining time
					}
					responseMsg = fmt.Sprintf("VALUE %v %v %v\r\n"+string(val.data)+"\r\n", val.version, t, val.numbytes)
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
						responseMsg = fmt.Sprintf("OK %v\r\n", version)
					} else {
						responseMsg = fmt.Sprintf("ERR_VERSION\r\n")
					}
				}
			}
		case handler.Delete:
			{
				if ok {
					delete(m, cmd.Key)
					responseMsg = "DELETED\r\n"
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
				responseMsg = "CLEANED\r\n"
			}
		default:
			{
				responseMsg = "ERR_INTERNAL\r\n"
			}
		}

		if cmd.Action != handler.Cleanup {
			// Send response to appropriate handler's channel
			util.ResponseChannelStore.RLock()
			responseChannel := util.ResponseChannelStore.M[logEntry.Lsn()]
			util.ResponseChannelStore.RUnlock()

			if responseChannel == nil {
				log.Printf("At server %d,  Response channel for LogEntry not found", serverID)
			} else {
				//Delete the entry for response channel handle
				util.ResponseChannelStore.Lock()
				delete(util.ResponseChannelStore.M, logEntry.Lsn())
				util.ResponseChannelStore.Unlock()
				*responseChannel <- responseMsg
			}
		}
	}
}

func cleaner(interval int, ch chan util.LogEntry) {
	command := handler.Command{handler.Cleanup, "", 0, 0, 0, nil}
	data, err := handler.EncodeCommand(command)
	logEntry := util.LogEntryObj{0, data, false, 0}
	if err != nil {
		log.Println("Error encoding the command ", err.Error())
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for range ticker.C {
		ch <- logEntry
	}
}