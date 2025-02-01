package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
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
		command, args, err := parseRESPCommand(reader)
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Println("Error parsing command: ", err)
			continue
		}

		response := handleCommand(command, args)
		if _, err := conn.Write([]byte(response)); err != nil {
			fmt.Println("Error writing response: ", err)
			break
		}
	}
}
