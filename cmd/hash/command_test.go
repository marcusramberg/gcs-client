package hash

import (
	"bytes"
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHashLocal(t *testing.T) {
	t.Parallel()
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "hash_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test file
	filename := "testfile.txt"
	filePath := filepath.Join(tmpDir, filename)
	content := []byte("hello world")
	if err := os.WriteFile(filePath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	// Calculate expected hashes
	m := md5.New() //nolint:gosec
	m.Write(content)
	expectedMD5 := base64.StdEncoding.EncodeToString(m.Sum(nil))

	c := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	c.Write(content)
	expectedCRC32C := fmt.Sprintf("%08x", c.Sum32())

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run hashLocal
	err = hashLocal(filePath, false)

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("hashLocal failed: %v", err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("Failed to read stdout: %v", err)
	}
	output := buf.String()

	// Verify output
	expectedOutputMD5 := fmt.Sprintf("MD5:    %s", expectedMD5)
	expectedOutputCRC := fmt.Sprintf("CRC32C: %s", expectedCRC32C)

	if !strings.Contains(output, expectedOutputMD5) {
		t.Errorf("Output missing MD5 hash. Got:\n%s\nExpected to contain: %s", output, expectedOutputMD5)
	}
	if !strings.Contains(output, expectedOutputCRC) {
		t.Errorf("Output missing CRC32C hash. Got:\n%s\nExpected to contain: %s", output, expectedOutputCRC)
	}
}

func TestHashLocal_FileNotFound(t *testing.T) {
	t.Parallel()
	err := hashLocal("non_existent_file.txt", false)
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestCommand_Metadata(t *testing.T) {
	t.Parallel()
	if Command.Name != "hash" {
		t.Errorf("Expected command name to be hash, got %s", Command.Name)
	}
	if len(Command.Flags) != 1 {
		t.Errorf("Expected 1 flag, got %d", len(Command.Flags))
	}
}
