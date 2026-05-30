package main

import (
	"fmt"
	"os"
	"compress/gzip"
	"io"
	"bufio"
)

func gzipFile(src, dst string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return fmt.Errorf("source file does not exist: %s", src)
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	gzipWriter := gzip.NewWriter(dstFile)
	defer gzipWriter.Close()

	// Use bufio to handle large files efficiently
	reader := bufio.NewReader(srcFile)
	writer := bufio.NewWriter(gzipWriter)

	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	if err := gzipWriter.Close(); err != nil {
		return err
	}

	return nil
}