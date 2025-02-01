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
	case "SET":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'SET' command\r\n"
		}

		storage.Store(args[0], args[1])

		return "+OK\r\n"
	case "GET":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'SET' command\r\n"
		}

		if value, ok := storage.Load(args[0]); ok {
			// Type assertion to string since we only store strings
			strValue := value.(string)
			return fmt.Sprintf("$%d\r\n%s\r\n", len(strValue), strValue)
		}

		return "$-1\r\n" // Null bulk string for missing keys
	default:
		return "-ERR unknown command '" + command + "'\r\n"
	}
}
