package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Parse RESP array
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Println("Error reading from connection: ", err.Error())
			break
		}

		// Check array length
		if len(line) == 0 || line[0] != '*' {
			continue
		}

		arraySize, err := strconv.Atoi(string(line[1:]))
		if err != nil || arraySize < 1 {
			continue
		}

		var command string
		var args []string

		for i := 0; i < arraySize; i++ {
			line, _, err = reader.ReadLine()
			if err != nil || len(line) < 1 || line[0] != '$' {
				break
			}

			strLength, err := strconv.Atoi(string(line[1:]))
			if err != nil || strLength < 0 {
				break
			}

			strBytes := make([]byte, strLength)
			_, err = io.ReadFull(reader, strBytes)
			if err != nil {
				break
			}

			reader.Discard(2)

			if i == 0 {
				command = strings.ToUpper(string(strBytes))
			} else {
				args = append(args, string(strBytes))
			}
		}

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) > 0 {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
				conn.Write([]byte(response))
			}
		}

	}
}
