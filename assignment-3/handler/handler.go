package handler

import (
	"bufio"
	"fmt"
	util "github.com/saurabh-hote/cs733/assignment-3/util"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

func StartConnectionHandler(serverID int, clientPort int, appendReqChannel chan util.Event, mutex *sync.Mutex) {

	sock, err := net.Listen("tcp", ":"+strconv.FormatInt(int64(clientPort), 10))
	if err != nil {
		return
	}
	for {
		conn, err := sock.Accept()
		if err != nil {
			return
		}
		go HandleConn(serverID, conn, appendReqChannel, mutex)
	}
}

func HandleConn(serverID int, conn net.Conn, appendReqChannel chan util.Event, mutex *sync.Mutex) {
	addr := conn.RemoteAddr()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	responseChannel := make(chan string)

	//launch client specific go routine for initiating the repsonse channel
	go pollAndReply(serverID, writer, addr, &responseChannel)

	for {
		//Command Prompt
		//		write(writer, addr, "kv@cs733 ~ $ ")	// The Command Prompt :)

		str, e := reader.ReadString('\n')
		if e != nil {
			//Read error
			log.Printf("At Server %d, ERROR reading -  %s", serverID, e.Error())
			break
		}

		//Scan next line
		str = strings.TrimRight(str, "\r\n")
		if str == "" {
			continue //Empty Command
		}

		cmd, e := Parse(str)
		if e != nil {
			responseChannel <- e.Error()
		} else {
			//Do work here
			if cmd.Action == util.Set || cmd.Action == util.Cas {
				buf := make([]byte, cmd.Numbytes)
				_, ed := io.ReadFull(reader, buf)
				if (ed) != nil {
					//Read error
					log.Printf("At Server %d, ERROR reading -  %s", serverID, ed.Error())
					break
				}
				tail, ed2 := reader.ReadString('\n')
				if (ed2) != nil {
					//Read error
					log.Printf("At Server %d, ERROR reading -  %s", serverID, ed2.Error())
					break
				}
				cmd.Data = buf
				if (strings.TrimRight(tail, "\r\n") != "") || (len(cmd.Data) != cmd.Numbytes) {
					responseChannel <- "ERR_CMD_ERR\r\n"
					continue
				}
			}

			data, err := util.EncodeCommand(cmd)

			if err != nil {
				log.Printf("At Server %d, ERROR in encoding to gob -  %s", serverID, err.Error())
			}

			//now create message for the shared log module
			message := util.Event{util.TypeClientAppendRequest, util.ClientAppendRequest{data, &responseChannel}}

			//Obtain a lock on the raft instance
			//mutex.Lock()
				
			//push the messgae onto the shared channel
			//This will block until previous request gets completed
			appendReqChannel <- message
		}
	}
	// Shut down the connection.
	log.Printf("At Server %d, Closing the client port.", serverID)

	//Remove the response channel from the map
	util.ResponseChannelStore.Lock()
	for key, value := range util.ResponseChannelStore.M {
		if *value == responseChannel {
			delete(util.ResponseChannelStore.M, key)
		}
	}
	util.ResponseChannelStore.Unlock()
	conn.Close()
}

func pollAndReply(serverID int, w *bufio.Writer, clientAddr net.Addr, responseChannel *chan string) {
	for {
		replyMessage := <-*responseChannel

		_, err := fmt.Fprintf(w, replyMessage)
		if err != nil {
			log.Printf("At Server %d, ERROR writing - %s: %s", serverID, clientAddr.String(), replyMessage)
		}
		err = w.Flush()
		if err != nil {
			log.Printf("At Server %d, ERROR flushing - %s: %s", serverID, clientAddr.String(), err.Error())
		}
	}
}
