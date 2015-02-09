package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var ts = strings.TrimSpace

// Raft setup
type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

func getClusterConfig() ClusterConfig {
	serverConfig := []ServerConfig{
		{1, "localhost", 5001, 6001},
		{2, "localhost", 5002, 6002},
		{3, "localhost", 5003, 6003},
		{4, "localhost", 5004, 6004},
		{5, "localhost", 5005, 6005}}
	clusterConfig := ClusterConfig{"/log", serverConfig}
	return clusterConfig
}

var cmdObjects = make([]*exec.Cmd, 0)
var cmdChannel = make(chan *exec.Cmd, 1)
var serverReplicas int

func init() {
	clusterConfig := getClusterConfig()
	data, _ := json.Marshal(clusterConfig)
	ioutil.WriteFile("config.json", data, 0644)

	serverReplicas = len(clusterConfig.Servers)
	programName := "server.go"
	var wg sync.WaitGroup
	index := 1
	for index <= serverReplicas {
		constIndex := index
		wg.Add(1)
		go func() {
			defer wg.Done()
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

}

func TestSet(t *testing.T) {
	numberOfClientThreads := 100

	for len(cmdObjects) < serverReplicas {
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(5000 * time.Millisecond)
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

	log.Println("Waiting for all server instances to stop.........")
	//kill server and go process
	killServers()
}

//client code for cheking multiple clients setting same key
//expects final version to be number of clients setting that key
func launchClient(t *testing.T, ch chan int) {

	// connect to the server
	c, err := net.Dial("tcp", "localhost:5001")
	if err != nil {
		c.Close()
	}
	// send the message
	msg := "set rdg 30 2\r\nrt\r\n"
	io.Copy(c, bytes.NewBufferString(msg))
	if err != nil {
		c.Close()
	}
	//log.Println("sent ", msg)

	//reading server response
	resp, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		c.Close()
	} else {
		//log.Println(ts(resp))
		if strings.Contains(ts(resp), "OK") {
			ch <- 1
		} else {
			ch <- 0
		}

	}
	c.Close()
}

func killServers() {
	for _, server := range cmdObjects {
		err := server.Process.Kill()
		if err != nil {
			log.Println("go routine Killing error")
		}
		server.Process.Wait()
	}
	if exec.Command("pkill", "server").Run() != nil {
		log.Println("Server Killing error")
	}
}
