//go:build pg_dump_embedded

package main

import "embed"

// IMPORTANTE:
// Para compilar com pg_dump embutido, coloque o binário em:
//   assets/pg_dump/pg_dump
// e compile com:
//   go build -tags pg_dump_embedded

//go:embed assets/pg_dump/pg_dump
var pgDumpFS embed.FS

func embeddedPGDumpBytes() ([]byte, bool) {
	b, err := pgDumpFS.ReadFile("assets/pg_dump/pg_dump")
	if err != nil || len(b) == 0 {
		return nil, false
	}
	return b, true
}

