package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Data struct {
	version    int64
	value      string
	expiryTime time.Time
}

var Store = struct {
	sync.RWMutex
	m map[string]Data
}{m: make(map[string]Data)}

type CommandInterface interface {
	Execute() string
}

type NullWriter int

type Command struct{ param []string }
type SetCommand struct {
	Command
	noreply bool
}
type GetCommand struct{ Command }
type CASCommand struct {
	Command
	noreply bool
}
type GetMCommand struct{ Command }
type DeleteCommand struct{ Command }
type InvalidCommand struct{ Command }

func (command SetCommand) Execute() string {
	var data Data
	var message string

	key := strings.Trim(command.param[0], "\r\n")
	expiryTime, expTimeErr := strconv.ParseInt(command.param[1], 10, 64)
	valueSize, valueSizeErr := strconv.ParseInt(command.param[2], 10, 32)
	if command.noreply {
		data.value = command.param[4]
	} else {
		data.value = command.param[3]
	}

	if expTimeErr != nil || valueSizeErr != nil ||
		len(key) > 250 || len(data.value) != int(valueSize) {
		message = "ERRCMDERR\r\n"
	} else {
		if expiryTime != 0 {
			data.expiryTime = time.Now().Add(time.Second * time.Duration(expiryTime))
		}
		Store.RLock()
		value, ok := Store.m[key]
		Store.RUnlock()

		if !ok || value.expiryTime.Before(time.Now()) {
			data.version = 1
		} else {
			data.version = value.version + 1
		}

		Store.Lock()
		Store.m[strings.Trim(command.param[0], "\r\n")] = data
		Store.Unlock()
		message = "OK " + strconv.FormatInt(data.version, 10) + "\r\n"
	}
	if command.noreply {
		message = ""
	}
	return message
}

func (command CASCommand) Execute() string {
	var data Data
	var message string

	key := strings.Trim(command.param[0], "\r\n")
	expiryTime, expTimeParseErr := strconv.ParseInt(command.param[1], 10, 64)
	version, versionParseErr := strconv.ParseInt(command.param[2], 10, 64)
	valueSize, valueSizeParseErr := strconv.ParseInt(command.param[3], 10, 32)

	if command.noreply {
		data.value = command.param[5]
	} else {
		data.value = command.param[4]
	}

	if expTimeParseErr != nil || valueSizeParseErr != nil || versionParseErr != nil ||
		len(key) > 250 || len(data.value) != int(valueSize) {
		message = "ERRCMDERR\r\n"
	} else {
		if expiryTime != 0 {
			data.expiryTime = time.Now().Add(time.Second * time.Duration(expiryTime))
		}
		Store.RLock()
		value, ok := Store.m[key]
		Store.RUnlock()

		if !ok || (!value.expiryTime.IsZero() && value.expiryTime.Before(time.Now())) {
			message = "ERRNOTFOUND\r\n"
		} else if value.version == version {
			data.version = value.version + 1
			Store.Lock()
			Store.m[key] = data
			Store.Unlock()
			message = "OK " + strconv.FormatInt(data.version, 10) + "\r\n"
		} else {
			message = "ERR_VERSION\r\n"
		}
	}
	if command.noreply {
		message = ""
	}
	return message
}

func (command GetCommand) Execute() string {
	Store.RLock()
	data, ok := Store.m[strings.Trim(command.param[0], "\r\n")]
	Store.RUnlock()

	if !ok || (!data.expiryTime.IsZero() && data.expiryTime.Before(time.Now())) {
		return "ERRNOTFOUND" + "\r\n"
	}
	return "VALUE " + strconv.Itoa(len(data.value)) + "\r\n" + data.value + "\r\n"
}

func (command GetMCommand) Execute() string {
	Store.RLock()
	data, ok := Store.m[strings.Trim(command.param[0], "\r\n")]
	Store.RUnlock()
	if !ok || (!data.expiryTime.IsZero() && data.expiryTime.Before(time.Now())) {
		return "ERRNOTFOUND\r\n"
	}
	var timeToExpire int64
	if data.expiryTime.IsZero() {
		timeToExpire = 0
	} else {
		timeToExpire = int64(data.expiryTime.Sub(time.Now()).Seconds())
	}
	return "VALUE " + strconv.FormatInt(data.version ,10) + " " +
		strconv.FormatInt(timeToExpire, 10) + " " + strconv.Itoa(len(data.value)) + "\r\n" + data.value + "\r\n"
}

func (command DeleteCommand) Execute() string {
	key := strings.Trim(command.param[0], "\r\n")
	Store.RLock()
	_, ok := Store.m[key]
	Store.RUnlock()

	var message string
	if ok {
		Store.Lock()
		delete(Store.m, key)
		Store.Unlock()
		message = "DELETED\r\n"
	} else {
		message = "ERRNOTFOUND\r\n"
	}
	return message
}

func (command InvalidCommand) Execute() string {
	return "ERRCMDERR\r\n"
}

func CleanUp() {
	ticker := time.NewTicker(time.Second * 120)
	currentTime := time.Now()
	for range ticker.C {
		Store.Lock()
		for key, value := range Store.m {
			if (!value.expiryTime.IsZero()) && value.expiryTime.Before(currentTime) {
				delete(Store.m, key)
			}
		}
		Store.Unlock()
	}
}

func RequestHandler(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		input := strings.TrimRight(string(line), "\r\n")

		log.Println("received ", input)

		array := strings.Split(input, " ")
		command := array[0]
		var commandObj CommandInterface

		if command == "set" && (len(array) == 4 || (len(array) == 5 && array[4] == "noreply")) {
			noreply := (len(array) == 5 && array[4] == "noreply")
			line, err = reader.ReadBytes('\n')

			if err != nil {
				break
			}
			array = append(array, strings.TrimRight(string(line), "\r\n"))
			commandObj = SetCommand{Command{array[1:]}, noreply}
		} else if command == "get" && len(array) == 2 {
			commandObj = GetCommand{Command{array[1:]}}
		} else if command == "getm" && len(array) == 2 {
			commandObj = GetMCommand{Command{array[1:]}}
		} else if command == "cas" && len(array) == 5 || (len(array) == 6 && array[5] == "noreply") {
			noreply := (len(array) == 6 && array[5] == "noreply")
			line, err = reader.ReadBytes('\n')
			if err != nil {
				break
			}
			array = append(array, strings.TrimRight(string(line), "\r\n"))
			commandObj = CASCommand{Command{array[1:]}, noreply}
		} else if command == "delete" && len(array) == 2 {
			commandObj = DeleteCommand{Command{array[1:]}}
		} else {
			commandObj = InvalidCommand{Command{array[:]}}
		}
		SendData(conn, commandObj.Execute())
	}
}

func SendData(conn net.Conn, message string) {
	if len(message) > 0 {
		log.Println("sent ", message)
		io.Copy(conn, bytes.NewBufferString(message))
	}
}

func (NullWriter) Write([]byte) (int, error) {
	return 0, nil
}

func main() {
	//Comment following line to print the logs on console
	log.SetOutput(new(NullWriter))

	psock, err := net.Listen("tcp", ":9000")
	if err != nil {
		return
	}
	go CleanUp()
	for {
		conn, err := psock.Accept()
		if err != nil {
			return
		}

		go RequestHandler(conn)
	}
}
