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
	_, err := readSizeEncoded(file)
	if err != nil {
		return err
	}

	if _, err := readSizeEncoded(file); err != nil {
		return err
	}

	// Skip 0xFB 0x01 if present
	b, err := readByte(file)
	if err != nil {
		return err
	}
	if b == 0xFB {
		_, err = readByte(file) // consume the 0x01
		if err != nil {
			return err
		}

		if _, err := readSizeEncoded(file); err != nil {
			return err
		}
	} else {
		// if it wasn't 0xFB, put the byte back for the next stage
		if _, err := file.Seek(-1, io.SeekCurrent); err != nil {
			return err
		}
		// Put the stream dict size byte back too!
		if _, err := file.Seek(-1, io.SeekCurrent); err != nil {
			return err
		}
		if _, err := readSizeEncoded(file); err != nil { // Stream dict size -- PROBLEM!
			return err
		}
	}

	for {
		var expiresAt time.Time

		b, err := readByte(file)
		if err != nil {
			return err
		}

		var valueType byte
		switch b {
		case 0xFD: // Expire time in seconds
			expiresBytes := make([]byte, 4)
			if _, err := io.ReadFull(file, expiresBytes); err != nil {
				return err
			}
			expiresAt = time.Unix(int64(binary.LittleEndian.Uint32(expiresBytes)), 0)

			valueType, err = readByte(file)
			if err != nil {
				return err
			}
		case 0xFC: // Expire time in milliseconds
			expiresBytes := make([]byte, 8)
			if _, err := io.ReadFull(file, expiresBytes); err != nil {
				return err
			}
			expiresAt = time.Unix(0, int64(binary.LittleEndian.Uint64(expiresBytes))*int64(time.Millisecond))

			valueType, err = readByte(file)
			if err != nil {
				return err
			}
		case 0xFF: // Eend of RDB file
			return nil
		default:
			// Put the byte back and continue
			valueType = b
		}

		// For this stage, we only handle string values (type 0)
		if valueType != 0 {
			return fmt.Errorf("unsuported value type: %x", valueType)
		}

		// Read key and value
		key, err := readStringEncoded(file)
		if err != nil {
			return err
		}
		value, err := readStringEncoded(file)
		if err != nil {
			return err
		}

		// Store in memory (only if not expired)
		if expiresAt.IsZero() || time.Now().Before(expiresAt) {
			storage.Store(key, storedValue{
				value:     value,
				expiresAt: expiresAt,
			})
		}
	}
}
