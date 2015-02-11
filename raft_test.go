package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	raft "github.com/swapniel99/cs733-raft/raft"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"math/rand"
)

var cmdObjects = make([]*exec.Cmd, 0)
var clusterConfig *raft.ClusterConfig
const noOfClientThreads int = 10
const noOfRequestsPerThread int = 10

func init() {
	var cmdChannel = make(chan *exec.Cmd, 1)
	serverConfig := []raft.ServerConfig{
		{1, "localhost", 5001, 6001},
		{2, "localhost", 5002, 6002},
		{3, "localhost", 5003, 6003},
		{4, "localhost", 5004, 6004},
		{5, "localhost", 5005, 6005}}
	clusterConfig = &raft.ClusterConfig{"/log", serverConfig}

	data, _ := json.Marshal(*clusterConfig)
	ioutil.WriteFile("config.json", data, 0644)

	serverReplicas := len((*clusterConfig).Servers)
	programName := "server.go"
	index := 1
	for index <= serverReplicas {
		constIndex := index
		go func() {
			cmd := exec.Command("go", "run", programName, "-id="+strconv.Itoa(constIndex))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmdChannel <- cmd
			err := cmd.Run()
			if err != nil {
				//log.Println("Server stopped :", constIndex, err.Error())
			}
		}()
		cmdobj := <-cmdChannel
		cmdObjects = append(cmdObjects, cmdobj)
		index++
	}
	log.Println("Waiting for all server instances to start.........")
	for len(cmdObjects) < serverReplicas {
		time.Sleep(100 * time.Millisecond)
	}
	waitTimeForServerReplicaStart := 5 * time.Second
	time.Sleep(waitTimeForServerReplicaStart)
}

/*
Test for checking the the correct version
The test execytes multiple set commands and then checks for the correct version on getm execute
*/
func TestEntryVersion(t *testing.T) {
	//start the server
	//go main()

	commands := []string {
		"set saurabh 60 7\r\nHoteABC\r\n",
		"set saurabh 60 7\r\nHotePQR\r\n",
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
	for connectionCounter < noOfClientThreads {
		go executeCommands(t, done, commands, 0)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfClientThreads {
		<-done
		connectionCounter += 1
	}

	//now verify the version
	versionReceived := executeGetMForVersion(t, verifyCommand)
	versionReceived++ //since the versioning starts from 0
	if noOfClientThreads*noOfRequestsPerThread == int(versionReceived) {
		log.Println("TestVersionMatch Passed.")
	} else {
		t.Error("TestVersionMatch Failed. Expected " + strconv.Itoa(noOfClientThreads*noOfRequestsPerThread) + " received " + strconv.FormatInt(versionReceived, 10))
	}
}

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
	for connectionCounter < noOfClientThreads {
		go executeCommands(t, done, commands, 0)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfClientThreads {
		<-done
		connectionCounter += 1
	}
	log.Println("TestConcurrency Passed.")
}

func TestKillServers(t *testing.T) {
	log.Println("Waiting for all server instances to stop.........")
	for _, cmd := range cmdObjects {
		err := cmd.Process.Kill()
		if err != nil {
			log.Println("go routine Killing error")
		}
		cmd.Process.Wait()
	}
	if runtime.GOOS == "windows" {
		exec.Command("taskkill", "/IM", "server.exe", "/F").Run()
	} else {
		exec.Command("pkill", "server").Run()
	}
}

//Miscellaneous functions
func executeCommands(t *testing.T, done chan bool, commands []string, execCount int) bool {
	conn, err := net.Dial("tcp", (*clusterConfig).Servers[0].Hostname + ":" + strconv.Itoa((*clusterConfig).Servers[0].ClientPort))
	log.Println("Conencted ")
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
			} else if array[0] != "OK" {
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
	conn, err := net.Dial("tcp", (*clusterConfig).Servers[0].Hostname + ":" + strconv.Itoa((*clusterConfig).Servers[0].ClientPort))
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
	conn, err := net.Dial("tcp", (*clusterConfig).Servers[0].Hostname + ":" + strconv.Itoa((*clusterConfig).Servers[0].ClientPort))
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
