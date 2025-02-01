package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

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

// parseCommandFromRESP parses a Redis RESP protocol message and returns the command + arguments
// RESP protocol reference: https://redis.io/docs/reference/protocol-spec/
func parseRESPCommand(reader *bufio.Reader) (string, []string, error) {
	// RESP commands come in array format: *<number-of-elements>\r\n<elements...>
	// Example: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n → ["ECHO", "hey"]

	// Read the first line which should be the array header
	headerLine, _, err := reader.ReadLine()
	if err != nil {
		return "", nil, err
	}

	// Array header must start with '*' and contain the number of elements
	if len(headerLine) == 0 || headerLine[0] != '*' {
		return "", nil, fmt.Errorf("invalid RESP array header")
	}

	// Convert the array size from string to integer (e.g., "*2" → 2)
	arraySize, err := parseRESPInteger(string(headerLine[1:]), 1, "invalid array size: %q (must be ≥1)")
	if err != nil {
		return "", nil, err
	}

	var command string
	var args []string

	// Process each element in the array
	for i := 0; i < arraySize; i++ {
		// Read bulk string header: $<length>\r\n
		bulkHeader, _, err := reader.ReadLine()
		if err != nil {
			return "", nil, err
		}

		// Bulk strings must start with '$' followed by their length
		if len(bulkHeader) < 1 || bulkHeader[0] != '$' {
			return "", nil, fmt.Errorf("invalid bulk string header")
		}

		// Convert length from string to integer (e.g., "$4" → 4)
		strLength, err := parseRESPInteger(string(bulkHeader[1:]), 0, "invalid bulk string length: %q (must be ≥0)")
		if err != nil {
			return "", nil, err
		}

		// Read exactly the specified number of bytes for the string content
		strBytes := make([]byte, strLength)
		_, err = io.ReadFull(reader, strBytes)
		if err != nil {
			return "", nil, err
		}

		// Discard the trailing \r\n after the bulk string content
		reader.Discard(2) // Remove the CRLF after the content

		// On first iteration we get the command name, and after, the arguments
		if i == 0 {
			command = strings.ToUpper(string(strBytes))
		} else {
			args = append(args, string(strBytes))
		}
	}

	return command, args, nil
}

// parseRESPInteger safely converts a RESP protocol integer string to an integer
// with validation. Used for both array sizes and bulk string lengths.
//
// Parameters:
// - s: The string to convert (e.g. "3" from "*3" or "5" from "$5")
// - min: Minimum allowed value (array sizes need min=1, bulk strings min=0)
// - errorFormat: Format string for error message (shows original value)
//
// Returns:
// - Parsed integer value
// - Error if conversion fails or value < min
func parseRESPInteger(s string, min int, errorFormat string) (int, error) {
	val, err := strconv.Atoi(s)
	if err != nil || val < min {
		return 0, fmt.Errorf(errorFormat, s)
	}

	return val, nil
}

// parseCommandExpiry handles the expiration time parsing for SET commands
// Arguments format: [key, value, "PX", milliseconds]
// Returns:
// - expiresAt: Time when the key should expire
// - errorResponse: RESP protocol error string if validation fails
func parseCommandExpiry(args []string) (expiresAt time.Time, errorResponse string) {
	// Validate we have exactly 4 arguments (key, value, option, time)
	if len(args) != 4 {
		return time.Time{},
			"-ERR wrong number of arguments for SET with expiry\r\n"
	}

	// Validate option is PX (case-insensitive)
	option := strings.ToUpper(args[2])
	if option != "PX" {
		return time.Time{},
			"-ERR unsupported option\r\n"
	}

	// Parse milliseconds value
	ms, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{},
			"-ERR invalid expiry time\r\n"
	}

	// Calculate absolute expiration time
	return time.Now().Add(time.Duration(ms) * time.Millisecond), ""
}
