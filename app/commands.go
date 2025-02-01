package main

import "fmt"

func handleCommand(command string, args []string) string {
	switch command {
	case "PING":
		return "+PONG\r\n"
	case "ECHO":
		if len(args) == 0 {
			return "-ERR wrong number of arguments for 'echo' command\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
	default:
		return "-ERR unknown command '" + command + "'\r\n"
	}
}
