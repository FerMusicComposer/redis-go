package main

import (
	"fmt"
	"strings"
)

type CommandHandler func(args []string) string

var registry = map[string]struct {
	minArgs int
	maxArgs int
	handler CommandHandler
}{
	"SET":    {2, 4, setCommand},
	"GET":    {1, 1, getCommand},
	"PING":   {0, 0, pingCommand},
	"ECHO":   {1, 1, echoCommand},
	"CONFIG": {2, 2, configCommand},
	"KEYS":   {1, 1, keysCommand},
}

func handleCommand(command string, args []string) string {
	cmd, exists := registry[strings.ToUpper(command)]
	if !exists {
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", command)
	}

	if len(args) < cmd.minArgs || len(args) > cmd.maxArgs {
		return fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", strings.ToUpper(command))
	}

	return cmd.handler(args)
}
