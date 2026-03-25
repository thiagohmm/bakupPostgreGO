//go:build !pg_dump_embedded

package main

func embeddedPGDumpAllBytes() ([]byte, bool) {
	return nil, false
}

