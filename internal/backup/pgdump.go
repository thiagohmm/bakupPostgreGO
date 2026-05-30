package backup

import (
	"fmt"
	"os"
	"os/exec"
)

// BackupDB realiza o backup do banco de dados PostgreSQL usando pg_dump.
// Parâmetros: host, database, user, password, output file.
// Retorna erro em caso de falha.
func BackupDB(host, db, user, password, output string) error {
	// Validação de parâmetros
	if host == "" || db == "" || user == "" || output == "" {
		return fmt.Errorf("todos os parâmetros são obrigatórios")
	}

	// Construção do comando pg_dump
	cmd := exec.Command("pg_dump", "-h", host, "-d", db, "-U", user, "-f", output)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	// Configurar a senha via variável de ambiente, se fornecida
	if password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+password)
	}

	// Execução do comando
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("falha ao executar pg_dump: %w", err)
	}

	return nil
}

// BackupAllDBs realiza o backup de todos os bancos de dados PostgreSQL usando pg_dumpall.
// Parâmetros: host, user, password, output file.
func BackupAllDBs(host, user, password, output string) error {
	// Validação de parâmetros
	if host == "" || user == "" || output == "" {
		return fmt.Errorf("todos os parâmetros são obrigatórios")
	}

	// Construção do comando pg_dumpall
	cmd := exec.Command("pg_dumpall", "-h", host, "-U", user, "-f", output)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	// Configurar a senha via variável de ambiente, se fornecida
	if password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+password)
	}

	// Execução do comando
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("falha ao executar pg_dumpall: %w", err)
	}

	return nil
}