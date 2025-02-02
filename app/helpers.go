package main

import (
	"bufio"
	"encoding/binary"
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

// readByte reads a single byte from the provided io.Reader.
//
// Parameters:
// - r: The io.Reader to read from.
//
// Returns:
// - The byte read from the reader.
// - An error if reading fails.
func readByte(r io.Reader) (byte, error) {
	b := make([]byte, 1)
	_, err := r.Read(b)
	return b[0], err
}

// readSizeEncoded reads a size-encoded integer from the provided io.Reader.
// The size is encoded in the first byte, with the two most significant bits
// indicating the encoding type:
//   - 00: The size is encoded in the lower 6 bits of the first byte (0-63).
//   - 01: The size is encoded in the lower 6 bits of the first byte, plus the next byte (64-16383).
//   - 10: The size is encoded in the next 4 bytes as a big-endian uint32.
//   - 11: Invalid encoding.
//
// Returns:
// - The decoded size as a uint32.
// - An error if reading fails or the encoding is invalid.
func readSizeEncoded(r io.Reader) (uint32, error) {
	// Read the first byte to determine the encoding type.
	firstByte, err := readByte(r)
	if err != nil {
		return 0, err
	}

	// Use a switch statement to handle different encoding types based on the first two bits.
	switch firstByte >> 6 {
	case 0:
		// If the first two bits are 00, the size is in the lower 6 bits of the first byte.
		// Mask the first byte with 0x3F (00111111) to get the size.
		return uint32(firstByte & 0x3F), nil
	case 1:
		// If the first two bits are 01, the size is in the lower 6 bits of the first byte and the next byte.
		// Read the second byte.
		secondByte, err := readByte(r)
		if err != nil {
			return 0, err
		}

		// Combine the lower 6 bits of the first byte (shifted left by 8 bits) with the second byte.
		return uint32(firstByte&0x3f)<<8 | uint32(secondByte), nil
	case 2:
		// If the first two bits are 10, the size is in the next 4 bytes as a big-endian uint32.
		// Create a byte slice to hold the next 4 bytes.
		bytes := make([]byte, 4)
		// Read the next 4 bytes into the byte slice.
		if _, err := io.ReadFull(r, bytes); err != nil {
			return 0, err
		}

		// Convert the 4 bytes to a uint32 using big-endian byte order.
		return binary.BigEndian.Uint32(bytes), nil
	default:
		// If the first two bits are 11, the encoding is invalid.
		return 0, fmt.Errorf("invalid size encoding")
	}
}

// readStringEncoded reads a string from the provided io.Reader.
// The string is encoded with a length prefix, which can be encoded in different ways.
// The encoding of the length is determined by the two most significant bits of the first byte.
// The string itself is encoded as a sequence of bytes.
//
// The length encoding is as follows:
//   - 00: The size is encoded in the lower 6 bits of the first byte (0-63).
//   - 01: The size is encoded in the lower 6 bits of the first byte, plus the next byte (64-16383).
//   - 10: The size is encoded in the next 4 bytes as a big-endian uint32.
//   - 11: The size is encoded in the lower 6 bits of the first byte, and the following bytes represent a number.
//   - 00: The next byte is a uint8.
//   - 01: The next 2 bytes are a little-endian uint16.
//   - 10: The next 4 bytes are a little-endian uint32.
//
// Returns:
// - The decoded string.
// - An error if reading fails or the encoding is invalid.
func readStringEncoded(r io.Reader) (string, error) {
	// Read the encoded length of the string.
	length, err := readSizeEncoded(r)
	if err != nil {
		// If there was an error reading the length, return an empty string and the error.
		return "", err
	}

	// Use a switch statement to handle different string encoding types based on the two most significant bits of the length.
	switch length >> 6 {
	case 0, 1, 2:
		// If the two most significant bits are 00, 01, or 10, the length is directly the length of the string.
		// Create a byte slice with the specified length.
		buf := make([]byte, length)
		// Read the exact number of bytes specified by the length into the buffer.
		if _, err := io.ReadFull(r, buf); err != nil {
			// If there was an error reading the bytes, return an empty string and the error.
			return "", err
		}

		// Convert the byte slice to a string and return it.
		return string(buf), nil
	case 3:
		// If the two most significant bits are 11, the lower 6 bits of the length determine the type of number to read.
		switch length & 0x3f {
		case 0:
			// If the lower 6 bits are 00, read a single byte and convert it to a string.
			b, err := readByte(r)
			// If there was an error reading the byte, return an empty string and the error.
			return strconv.Itoa(int(b)), err
		case 1:
			// If the lower 6 bits are 01, read 2 bytes and convert them to a little-endian uint16, then to a string.
			bytes := make([]byte, 2)
			if _, err := io.ReadFull(r, bytes); err != nil {
				// If there was an error reading the bytes, return an empty string and the error.
				return "", err
			}

			return strconv.Itoa(int(binary.LittleEndian.Uint16(bytes))), nil
		case 2:
			// If the lower 6 bits are 10, read 4 bytes and convert them to a little-endian uint32, then to a string.
			bytes := make([]byte, 4)
			if _, err := io.ReadFull(r, bytes); err != nil {
				// If there was an error reading the bytes, return an empty string and the error.
				return "", err
			}

			return strconv.Itoa(int(binary.LittleEndian.Uint32(bytes))), nil
		default:
			// If the lower 6 bits are not 00, 01, or 10, the encoding is unsupported.
			return "", fmt.Errorf("unsuported string encoding")
		}
	default:
		// If the two most significant bits are not 00, 01, 10, or 11, the encoding is invalid.
		return "", fmt.Errorf("invalid string encoding")
	}
}
