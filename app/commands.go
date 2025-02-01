package main

import (
	"fmt"
	"time"
)

type storedValue struct {
	value     string
	expiresAt time.Time
}

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

		var expiresAt time.Time
		var errStr string

		if len(args) == 4 {
			expiresAt, errStr = parseCommandExpiry(args)
			if errStr != "" {
				return errStr
			}
		}

		storage.Store(args[0], &storedValue{
			value:     args[1],
			expiresAt: expiresAt,
		})

		return "+OK\r\n"

	case "GET":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'SET' command\r\n"
		}

		val, ok := storage.Load(args[0])
		if !ok {
			return "$-1\r\n"
		}

		// Type assertion and expiration check
		sv, ok := val.(*storedValue)
		if !ok {
			storage.Delete(args[0]) // Clean up invalid data
			return "$-1\r\n"
		}

		// Check expiration if set
		if !sv.expiresAt.IsZero() && time.Now().After(sv.expiresAt) {
			storage.Delete(args[0])
			return "$-1\r\n" // Null bulk string for missing keys
		}

		return fmt.Sprintf("$%d\r\n%s\r\n", len(sv.value), sv.value)

	default:
		return "-ERR unknown command '" + command + "'\r\n"
	}
}
