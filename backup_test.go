package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGzipFile(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "test_backup")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("SuccessCompression", func(t *testing.T) {
		srcPath := filepath.Join(tmpDir, "test_src.txt")
		dstPath := filepath.Join(tmpDir, "test_dst.gz")
		content := []byte("Hello, this is a test content for gzip.")

		if err := os.WriteFile(srcPath, content, 0644); err != nil {
			t.Fatalf("Failed to write source file: %v", err)
		}

		if err := gzipFile(srcPath, dstPath); err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if _, err := os.Stat(dstPath); os.IsNotExist(err) {
			t.Errorf("Destination file was not created")
		}
	})

	t.Run("EmptySourceFile", func(t *testing.T) {
		srcPath := filepath.Join(tmpDir, "empty_src.txt")
		dstPath := filepath.Join(tmpDir, "empty_dst.gz")

		if err := os.WriteFile(srcPath, []byte(""), 0644); err != nil {
			t.Fatalf("Failed to write empty source file: %v", err)
		}

		if err := gzipFile(srcPath, dstPath); err != nil {
			t.Errorf("Expected no error for empty file, got: %v", err)
		}

	})

	t.Run("DestinationIsDir", func(t *testing.T) {
		srcPath := filepath.Join(tmpDir, "dir_src.txt")
		dstPath := filepath.Join(tmpDir, "a_directory")

		if err := os.Mkdir(dstPath, 0755); err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}
		if err := os.WriteFile(srcPath, []byte("content"), 0644); err != nil {
			t.Fatalf("Failed to write source: %v", err)
		}

		err := gzipFile(srcPath, dstPath)
		if err == nil {
			t.Errorf("Expected error when destination is a directory, but got nil")
		}
	})
}
