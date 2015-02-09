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
)

var cmdObjects = make([]*exec.Cmd, 0)
var clusterConfig *raft.ClusterConfig

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

func TestSet(t *testing.T) {
	log.Println("Executing TestSet")

	numberOfClientThreads := 10

	ch := make(chan int, 1)

	for i := 0; i < numberOfClientThreads; i++ {
		go launchClient(t, ch)
	}

	OkResponses := 0
	for i := 0; i < numberOfClientThreads; i++ {
		OkResponses += <-ch
	}
	if OkResponses == numberOfClientThreads {
		log.Println("TestSet passed.")
	} else {
		t.Error("Expected 'OK 100' but recieved OK ", OkResponses)
	}
}

//client code for cheking multiple clients setting same key
//expects final version to be number of clients setting that key
func launchClient(t *testing.T, ch chan int) {
	// connect to the server
	conn, err := net.Dial("tcp", (*clusterConfig).Servers[0].Hostname + ":" + strconv.Itoa((*clusterConfig).Servers[0].ClientPort))
	//conn, err := net.Dial("tcp", "localhost:5001")
	if err != nil {
		conn.Close()
	}

	msg := "set rdg 30 2\r\nrt\r\n"
	io.Copy(conn, bytes.NewBufferString(msg))
	if err != nil {
		conn.Close()
	}
	//log.Println("sent ", msg)

	//reading server response
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
	} else {
		//log.Println(ts(resp))
		if strings.Contains(strings.TrimSpace(resp), "OK") {
			ch <- 1
		} else {
			ch <- 0
		}

	}
	conn.Close()
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
