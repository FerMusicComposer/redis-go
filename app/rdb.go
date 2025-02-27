package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func loadRDBFile() error {
	path := filepath.Join(config.dir, config.dbFilename)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("error opening RDB file: %w", err)
	}
	defer file.Close()

	header := make([]byte, 9)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("error reading header: %w", err)
	}

	if !bytes.Equal(header, []byte("REDIS0011")) {
		return fmt.Errorf("invalid RDB header: %q", header)
	}

	for {
		b, err := readByte(file)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		switch b {
		case 0xFA:
			if err := parseMetadata(file); err != nil {
				return err
			}
		case 0xFE:
			if err := parseDatabase(file); err != nil {
				return err
			}
			return nil
		case 0xFF:
			return nil
		default:
			return fmt.Errorf("unexpected byte: %x", b)
		}
	}

	return nil
}

func parseMetadata(file io.Reader) error {
	// Metadata format:
	// [FA] [name (string encoded)] [value (string encoded)]
	name, err := readStringEncoded(file)
	if err != nil {
		return fmt.Errorf("metadata name read error: %w", err)
	}

	value, err := readStringEncoded(file)
	if err != nil {
		return fmt.Errorf("metadata value read error: %w", err)
	}

	// discarding for current stage
	_ = name
	_ = value

	return nil
}

func parseDatabase(file io.ReadSeeker) error {
	// Step 1: Read database selector (0xFE 0x00)
	// This indicates the start of a new database in the RDB file
	_, err := readSizeEncoded(file)
	if err != nil {
		return fmt.Errorf("failed to read database selector: %w", err)
	}

	// Step 2: Handle redis-bits metadata (0xC0 0x40)
	// This metadata indicates the number of bits used for encoding
	b, err := readByte(file)
	if err != nil {
		return fmt.Errorf("failed to read redis-bits metadata byte: %w", err)
	}
	if b == 0xC0 { // Special encoding for redis-bits
		// Read the value byte (0x40 in this case)
		valueByte, err := readByte(file)
		if err != nil {
			return fmt.Errorf("failed to read redis-bits value byte: %w", err)
		}
		// Log the redis-bits value for debugging
		fmt.Printf("DEBUG: Found redis-bits metadata with value: 0x%x\n", valueByte)
	} else {
		// If it wasn't 0xC0, put the byte back for the next stage
		if _, err := file.Seek(-1, io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to seek back after redis-bits check: %w", err)
		}
	}

	// Step 3: Handle stream dictionary (0xFB 0x01)
	// This indicates the presence of a stream dictionary in the RDB file
	b, err = readByte(file)
	if err != nil {
		return fmt.Errorf("failed to read stream dictionary marker: %w", err)
	}
	if b == 0xFB {
		// Consume the 0x01 byte
		_, err = readByte(file)
		if err != nil {
			return fmt.Errorf("failed to read stream dictionary value byte: %w", err)
		}

		// Read the size of the stream dictionary
		streamDictSize, err := readSizeEncoded(file)
		if err != nil {
			return fmt.Errorf("failed to read stream dictionary size: %w", err)
		}

		// Skip over the stream dictionary data
		_, err = file.Seek(int64(streamDictSize), io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to skip stream dictionary data: %w", err)
		}

		fmt.Printf("DEBUG: Skipped stream dictionary of size: %d bytes\n", streamDictSize)
	} else {
		// If it wasn't 0xFB, put the byte back for the next stage
		if _, err := file.Seek(-1, io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to seek back after stream dictionary check: %w", err)
		}
	}

	// Step 4: Parse key-value pairs
	// This is the main data section of the RDB file
	for {
		var expiresAt time.Time

		// Read the next byte to determine the type of entry
		b, err := readByte(file)
		if err != nil {
			return fmt.Errorf("failed to read entry type byte: %w", err)
		}

		var valueType byte
		switch b {
		case 0xFD: // Expire time in seconds
			// Read 4 bytes for the expiration timestamp
			expiresBytes := make([]byte, 4)
			if _, err := io.ReadFull(file, expiresBytes); err != nil {
				return fmt.Errorf("failed to read expiration time (seconds): %w", err)
			}
			expiresAt = time.Unix(int64(binary.LittleEndian.Uint32(expiresBytes)), 0)
			fmt.Printf("DEBUG: Found key with expiration (seconds): %v\n", expiresAt)

			// Read the value type byte
			valueType, err = readByte(file)
			if err != nil {
				return fmt.Errorf("failed to read value type after expiration: %w", err)
			}

		case 0xFC: // Expire time in milliseconds
			// Read 8 bytes for the expiration timestamp
			expiresBytes := make([]byte, 8)
			if _, err := io.ReadFull(file, expiresBytes); err != nil {
				return fmt.Errorf("failed to read expiration time (milliseconds): %w", err)
			}
			expiresAt = time.Unix(0, int64(binary.LittleEndian.Uint64(expiresBytes))*int64(time.Millisecond))
			fmt.Printf("DEBUG: Found key with expiration (milliseconds): %v\n", expiresAt)

			// Read the value type byte
			valueType, err = readByte(file)
			if err != nil {
				return fmt.Errorf("failed to read value type after expiration: %w", err)
			}

		case 0xFF: // End of RDB file
			fmt.Println("DEBUG: Reached end of RDB file")
			return nil

		default:
			// If it's not an expiration marker, treat it as the value type
			valueType = b
		}

		// Step 5: Validate value type
		// For this stage, we only handle string values (type 0)
		if valueType != 0 {
			return fmt.Errorf("unsupported value type: 0x%x", valueType)
		}

		// Step 6: Read key and value
		key, err := readStringEncoded(file)
		if err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}
		value, err := readStringEncoded(file)
		if err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}

		fmt.Printf("DEBUG: Loaded key-value pair: key=%s, value=%s\n", key, value)

		// Step 7: Store in memory (only if not expired)
		if expiresAt.IsZero() || time.Now().Before(expiresAt) {
			storage.Store(key, storedValue{
				value:     value,
				expiresAt: expiresAt,
			})
		} else {
			fmt.Printf("DEBUG: Skipped expired key: %s\n", key)
		}
	}
}
