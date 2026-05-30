//go:build !pg_dump_embedded

package backup

func EmbeddedPGDumpAllBytes() ([]byte, bool) {
	return nil, false
}

