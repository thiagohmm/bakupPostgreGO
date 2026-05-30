package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestGzipFile(t *testing.T) {
    // create a temporary source file
    srcPath := filepath.Join(os.TempDir(), "src.txt")
    dstPath := filepath.Join(os.TempDir(), "dst.txt.gz")
    defer os.Remove(srcPath)
    defer os.Remove(dstPath)
    content := []byte("hello world")
    if err := os.WriteFile(srcPath, content, 0o600); err != nil {
        t.Fatalf("write src failed: %v", err)
    }
    if err := gzipFile(srcPath, dstPath); err != nil {
        t.Fatalf("gzipFile failed: %v", err)
    }
    // read back and decompress
    f, err := os.Open(dstPath)
    if err != nil {
        t.Fatalf("open gzip file failed: %v", err)
    }
    gz, err := gzip.NewReader(f)
    if err != nil {
        t.Fatalf("gzip.NewReader failed: %v", err)
    }
    decoded, err := io.ReadAll(gz)
    if err != nil {
        t.Fatalf("read gzip content failed: %v", err)
    }
    if !bytes.Equal(decoded, content) {
        t.Fatalf("decompressed content mismatch, got %s, want %s", string(decoded), string(content))
    }
    gz.Close()
    f.Close()
}
