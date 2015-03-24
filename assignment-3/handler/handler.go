package handler

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	util "github.com/saurabh-hote/cs733/assignment-3/util"

)

const (
	Set = iota
	Get
	Getm
	Cas
	Delete
	Cleanup
)

type Command struct {
	Action   int
	Key      string
	Expiry   int64
	Version  uint64
	Numbytes int
	Data     []byte
}


func StartConnectionHandler(clientPort int, appendReqChannel chan util.Event) {
	sock, err := net.Listen("tcp", ":"+strconv.FormatInt(int64(clientPort), 10))
	if err != nil {
		return
	}
	for {
		conn, err := sock.Accept()
		if err != nil {
			return
		}
		go HandleConn(conn, appendReqChannel)
	}
}

func HandleConn(conn net.Conn, appendReqChannel chan util.Event) {
	addr := conn.RemoteAddr()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	responseChannel := make(chan string)

	//launch client specific go routine for initiating the repsonse channel
	go pollAndReply(writer, addr, &responseChannel)

	for {
		//Command Prompt
		//		write(writer, addr, "kv@cs733 ~ $ ")	// The Command Prompt :)

		str, e := reader.ReadString('\n')
		if e != nil {
			//Read error
			log.Println("ERROR reading:", addr, e)
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
			if cmd.Action == Set || cmd.Action == Cas {
				buf := make([]byte, cmd.Numbytes)
				_, ed := io.ReadFull(reader, buf)
				if (ed) != nil {
					//Read error
					log.Println("ERROR reading data:", addr, ed)
					break
				}
				tail, ed2 := reader.ReadString('\n')
				if (ed2) != nil {
					//Read error
					log.Println("ERROR reading post-data:", addr, ed2)
					break
				}
				cmd.Data = buf
				if (strings.TrimRight(tail, "\r\n") != "") || (len(cmd.Data) != cmd.Numbytes) {
					responseChannel <- "ERR_CMD_ERR\r\n"
					continue
				}
			}

			data, err := EncodeCommand(cmd)

			if err != nil {
				log.Println("ERROR in encoding to gob:", err)
			}

			//now create message for the shared log module
			message := util.Event{util.TypeClientAppendRequest, util.ClientAppendRequest{data, &responseChannel}}

			//push the messgae onto the shared channel
			//This will block until previous request gets completed
			appendReqChannel <- message
		}
	}
	// Shut down the connection.
	log.Println("Closing connection", addr)
	conn.Close()
}

//Encode using gob
func EncodeCommand(cmd Command) ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(cmd)
	return buff.Bytes(), err
}

//Decode using gob
func DecodeCommand(data []byte) (Command, error) {
	var buff bytes.Buffer
	buff.Write(data)
	enc := gob.NewDecoder(&buff)
	var command Command
	err := enc.Decode(&command)
	return command, err
}

/*
//Writes in TCP connection
func write(w *bufio.Writer, a net.Addr, s string) {
	_, err := fmt.Fprintf(w, s)
	if err != nil {
		log.Println("ERROR writing:", a, err)
	}
	err = w.Flush()
	if err != nil {
		log.Println("ERROR flushing:", a, err)
	}
}
*/

func pollAndReply(w *bufio.Writer, clientAddr net.Addr, responseChannel *chan string) {
	for {
		replyMessage := <-*responseChannel

		_, err := fmt.Fprintf(w, replyMessage)
		if err != nil {
			log.Println("ERROR writing:", clientAddr, replyMessage)
		}
		err = w.Flush()
		if err != nil {
			log.Println("ERROR flushing:", clientAddr, err)
		}
	}
}