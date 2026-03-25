//go:build !pg_dump_embedded

package main

// embeddedPGDumpBytes retorna o conteúdo do pg_dump embutido, se disponível.
// Nesta variante (sem a tag pg_dump_embedded), não há binário embutido.
func embeddedPGDumpBytes() ([]byte, bool) {
	return nil, false
}

