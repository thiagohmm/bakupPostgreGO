//go:build pg_dump_embedded

package main

import "embed"

//go:embed assets/pg_dumpall/pg_dumpall
var pgDumpAllFS embed.FS

func embeddedPGDumpAllBytes() ([]byte, bool) {
	b, err := pgDumpAllFS.ReadFile("assets/pg_dumpall/pg_dumpall")
	if err != nil || len(b) == 0 {
		return nil, false
	}
	return b, true
}

