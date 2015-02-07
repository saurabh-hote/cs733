package handler

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

type Command struct {
	Action   int
	Key      string
	Expiry   int64
	Version  uint64
	Numbytes int
	Data     []byte
}

func handleConn(conn net.Conn) {
	addr := conn.RemoteAddr()
	log.Println(addr, "connected.")
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

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
			write(writer, addr, e.Error())
		} else {
			//Do work here
			if cmd.Action == 0 || cmd.Action == 3 {
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
					write(writer, addr, "ERR_CMD_ERR\r\n")
					continue
				}
			}

			data, err := encode(cmd)
			if err != nil {
				log.Println("ERROR in encoding to gob:", err)
			}

			fmt.Println(data) // Delete this line. data contains the encoded command to be sent to Append.

			//			ch <- data		// Use it

			//			reply := <-resp	// Get response

			//			write(writer, addr, reply)	//	Write response on TCP conn
		}
	}
	// Shut down the connection.
	log.Println("Closing connection", addr)
	conn.Close()
}

//Encode using gob
func encode(cmd Command) ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(cmd)
	return buff.Bytes(), err
}

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
