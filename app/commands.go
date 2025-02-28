package main

import (
	"fmt"
	"strings"
	"time"
)

type storedValue struct {
	value     string
	expiresAt time.Time
}

func pingCommand(args []string) string {
	_ = args
	return "+PONG\r\n"
}

func echoCommand(args []string) string {
	if len(args) == 0 {
		return "-ERR wrong number of arguments for 'echo' command\r\n"
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
}

func setCommand(args []string) string {
	if len(args) != 2 && len(args) != 4 {
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
}

func getCommand(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
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
}

func configCommand(args []string) string {
	subCommand := strings.ToUpper(args[0])
	parameter := strings.ToLower(args[1])

	switch subCommand {
	case "GET":
		switch parameter {
		case "dir":
			return fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n",
				len(config.dir), config.dir)
		case "dbfilename":
			return fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n",
				len(config.dbFilename), config.dbFilename)
		default:
			return "*0\r\n"
		}
	default:
		return "-ERR unknown subcommand\r\n"
	}
}

// keysCommand handles the KEYS command which returns all keys matching a pattern.
// Currently, it only supports the "*" pattern which matches all keys.
//
// Parameters:
//   - args: Command arguments where args[0] is the pattern to match
//
// Returns:
//   - RESP (Redis Serialization Protocol) formatted string:
//   - For successful match: "*<count>\r\n$<length>\r\n<key>\r\n..." for each key
//   - For unsupported pattern: "-ERR pattern not supported\r\n"
//
// Example:
//
//	Input: ["*"]
//	Output: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n" (for keys "foo" and "bar")
func keysCommand(args []string) string {
	if args[0] != "*" {
		return "-ERR pattern not supported\r\n"
	}

	var keys []string
	storage.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})

	resp := fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}

	return resp
}
