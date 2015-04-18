package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

var noOfThreads int = 200
var noOfRequestsPerThread int = 10

/*
Test for concurrency
The test executes multiple commands concurrently 
*/
func TestConcurrency(t *testing.T) {
	//start the server
	//go main()

	commands := []string {
		"set saurabh 60 7\r\nHoteABC\r\n",
		"set ganesh 60 2\r\nAA\r\n",
		"cas saurabh 60 1 7\r\nHotePQR\r\n",
		"set saurabh 60 7\r\nHotePQR\r\n",
		"delete saurabh\r\n",
		"delete ganesh\r\n",
		"getm ganesh\r\n",
		"getm saurabh\r\n",
	}

	done := make(chan bool)

	//spwan client threads
	connectionCounter := 0
	for connectionCounter < noOfThreads {
		go executeCommands(t, done, commands, 0)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfThreads {
		<-done
		connectionCounter += 1
	}
	t.Skip("Passed")
}

/*
Test for checking the the correct version
The test execytes multiple set commands and then checks for the correct version on getm execute
*/
func TestEntryVersion(t *testing.T) {
	//start the server
	//go main()

	commands := []string {
		"set saurabh 60 7 noreply\r\nHoteABC\r\n",
		"set saurabh 60 7 noreply\r\nHotePQR\r\n",
	}
	deleteCommand := "delete saurabh\r\n"
	verifyCommand := "getm saurabh\r\n"

	done := make(chan bool)

	//delete the key to test
	if executeDelete(t, deleteCommand) {
		t.Error("Error executing command " + deleteCommand)
	}

	//spwan client threads
	connectionCounter := 0
	for connectionCounter < noOfThreads {
		go executeCommands(t, done, commands, 0)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfThreads {
		<-done
		connectionCounter += 1
	}

	//now verify the version
	versionReceived := executeGetMForVersion(t, verifyCommand)
	if connectionCounter*noOfRequestsPerThread == int(versionReceived) {
		t.Skip("Version Matched")
	} else {
		t.Error("Version Match Failed")
	}
}

/*
Test for checking the validity of expiry value on getm command
The test execytes multiple set commands, waits for value expiry duration and 
checks for the expiry value error
*/
func TestEntryExpiry(t *testing.T) {
	//start the server
	//go main()
	expiryTime := 10
	commands := []string{
		"set saurabh " + strconv.Itoa(expiryTime) + " 7 noreply\r\nHoteABC\r\n",
		"set saurabh " + strconv.Itoa(expiryTime) + " 7 noreply\r\nHotePQR\r\n",
	}
	verifyCommand := "get saurabh\r\n"

	done := make(chan bool)

	//spwan client threads
	connectionCounter := 0
	for connectionCounter < noOfThreads {
		go executeCommands(t, done, commands, 0)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfThreads {
		<-done
		connectionCounter += 1
	}

	//now wait for the expiry time set on last command
	log.Println("Waiting for expiry peroid")
	time.Sleep(time.Duration(expiryTime+1) * time.Second)

	//now check the key status
	retVal := executeGetMForVersion(t, verifyCommand)
	if retVal == -1 {
		t.Skip("Key expiry test passed")
	} else {
		t.Error("Key not expired")
	}
}

/*
Test for checking the validity of CAS command execution
The test tries to execute CAS with invalid and vaild key version value
*/
func TestCAS(t *testing.T) {
	//start the server
	//go main()
	commands := []string{
		"set saurabh 60 7 noreply\r\nHoteABC\r\n",
		"set saurabh 60 7 noreply\r\nHotePQR\r\n",
	}

	done := make(chan bool, 10)

	//spwan client threads
	connectionCounter := 0
	for connectionCounter < noOfThreads {
		go executeCommands(t, done, commands, 0)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfThreads {
		<-done
		connectionCounter += 1
	}

	//now execute CAS command with possibly wrong version number
	casCommandInvalid := []string{
		"cas saurabh 60 1 7\r\nHotePQR\r\n",
	}
	if executeCommands(t, done, casCommandInvalid, 1) {
		t.Error("CAS with incorrect version got executed")
	}
	//now execute CAS command with right version number
	casCommand := []string{
		"cas saurabh 60 " + strconv.Itoa(noOfRequestsPerThread*noOfThreads) + " 7\r\nHotePQR\r\n",
	}
	if executeCommands(t, done, casCommand, 1) {
		t.Skip("CAS correctly executed")
	}
}

//Miscellaneous functions
func executeCommands(t *testing.T, done chan bool, commands []string, execCount int) bool {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- false
		return false
	}

	connbuf := bufio.NewReader(conn)
	randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	var line []byte
	result := true
	executionCount := noOfRequestsPerThread
	if execCount > 0 {
		executionCount = execCount
	}

	j := 0
	for j < executionCount {
		commandIndex := randomGenerator.Int() % len(commands)
		io.Copy(conn, bytes.NewBufferString(commands[commandIndex]))
		log.Println("sent ", commands[commandIndex])

		if !strings.HasSuffix(strings.Split(commands[commandIndex], "\r\n")[0], "noreply") {

			line, err = connbuf.ReadBytes('\n')
			input := strings.TrimRight(string(line), "\r\n")
			log.Println("received ", input)
			array := strings.Split(input, " ")

			if array[0] == "VALUE" {
				line, err = connbuf.ReadBytes('\n')
				input = strings.TrimRight(string(line), "\r\n")
				log.Println(input)

				if err != nil {
					result = false
					break
				}
			} else {
				result = false
				break
			}
		}
		j += 1
	}

	if err != nil {
		t.Error(err)
	} 
	done <- result
	conn.Close()
	return result
}

func executeGetMForVersion(t *testing.T, command string) int64 {
	var version int64
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		return -1
	}

	connbuf := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString(command))
	log.Println("sent ", command)

	line, _ := connbuf.ReadBytes('\n')
	input := strings.TrimRight(string(line), "\r\n")
	log.Println("received ", input)

	array := strings.Split(input, " ")
	result := array[0]
	if result == "VALUE" {
		line, err = connbuf.ReadBytes('\n')
		version, _ = strconv.ParseInt(array[1], 10, 64)
		if err != nil {
			version = -1
		}
	} else {
		version = -1
	}

	if err != nil {
		t.Error(err)
	}
	conn.Close()
	return version
}

func executeDelete(t *testing.T, command string) bool {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		return false
	}

	connbuf := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString(command))
	log.Println("sent ", command)

	line, _ := connbuf.ReadBytes('\n')
	input := strings.TrimRight(string(line), "\r\n")
	log.Println("received ", input)
	conn.Close()

	if input == "DELETED" {
		t.Skip("Delete Passed")
		return true

	} else {
		return false
	}
}
