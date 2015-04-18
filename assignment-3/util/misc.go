package util

import (
	"sync"
)

var ResponseChannelStore = struct {
	sync.RWMutex
	M map[Lsn]*chan string
}{M: make(map[Lsn]*chan string)}
