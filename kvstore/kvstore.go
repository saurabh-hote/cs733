package kvstore

import (
	"container/heap"
	"fmt"
	"time"
)

const (
	Set = iota
	Get
	Getm
	Cas
	Delete
	Cleanup
)

type command struct {
	action   int
	key      string
	expiry   int64
	version  uint64
	numbytes int
	data     []byte
}

type value struct {
	data     []byte
	numbytes int
	version  uint64
	expiry   int64
}

//Map Manager
func mapman(ch chan *command) {	//	This channel has to be of type MyLogEntry. This is commitCh
	//The map which actually stores values
	m := make(map[string]value)
	h := &nodeHeap{}
	var counter uint64 = 0
	go cleaner(1, ch)
	for cmd := range ch {
		r := "ERR_NOT_FOUND\r\n"
		val, ok := m[cmd.key]
		switch cmd.action {
		case Set:
			{
				version := counter
				counter++
				t := cmd.expiry
				if t != 0 {
					t += time.Now().Unix()
				}
				m[cmd.key] = value{cmd.data, cmd.numbytes, version, t}
				if cmd.expiry != 0 {
					heap.Push(h, node{t, cmd.key, version})
				}
				r = fmt.Sprintf("OK %v\r\n", version)
			}
		case Get:
			{
				if ok {
					r = fmt.Sprintf("VALUE %v\r\n"+string(val.data)+"\r\n", val.numbytes)
				}
			}
		case Getm:
			{
				if ok {
					t := val.expiry
					if t != 0 {
						t = val.expiry - time.Now().Unix() // remaining time
					}
					if t < 0 {
						t = 0
					}
					r = fmt.Sprintf("VALUE %v %v %v\r\n"+string(val.data)+"\r\n", val.version, t, val.numbytes)
				}
			}
		case Cas:
			{
				if ok {
					if val.version == cmd.version {
						t := cmd.expiry
						if t != 0 {
							t += time.Now().Unix()
						}
						version := counter
						counter++
						m[cmd.key] = value{cmd.data, cmd.numbytes, version, t}
						if cmd.expiry != 0 {
							heap.Push(h, node{t, cmd.key, version})
						}
						r = fmt.Sprintf("OK %v\r\n", version)
					} else {
						r = fmt.Sprintf("ERR_VERSION\r\n")
					}
				}
			}
		case Delete:
			{
				if ok {
					delete(m, cmd.key)
					r = "DELETED\r\n"
				}
			}
		case Cleanup:
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
		fmt.Println(r)	//	Delete this line
//		cmd.resp <- r	// Send response to appropriate handler here.
	}
}

func cleaner(interval int, ch chan<- *command) {
	c := command{5, "", 0, 0, 0, nil}
	for {
		time.Sleep(time.Duration(interval) * time.Second)
		ch <- &c
	}
}
