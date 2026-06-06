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

	// Determinar qual binário usar
	_, ok := EmbeddedPGDumpBytes()
	var cmd *exec.Cmd
	if ok {
		// Se estiver usando o embutido, precisamos de uma forma de executá-lo.
		// Como o binário está embutido em bytes, não podemos usar exec.Command diretamente
		// sem escrever para um arquivo temporário ou usar uma técnica de execução de memória.
		// Para simplificar e manter a compatibilidade com o que já existe, 
		// vamos verificar se o binário do sistema existe primeiro.
		if _, err := exec.LookPath("pg_dump"); err == nil {
			cmd = exec.Command("pg_dump", "-h", host, "-d", db, "-U", user, "-f", output)
		} else {
			return fmt.Errorf("binário pg_dump não encontrado no sistema e não pode ser executado do embutido")
		}
	} else {
		cmd = exec.Command("pg_dump", "-h", host, "-d", db, "-U", user, "-f", output)
	}

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

	// Determinar qual binário usar
	_, ok := EmbeddedPGDumpBytes()
	var cmd *exec.Cmd
	if ok {
		if _, err := exec.LookPath("pg_dumpall"); err == nil {
			cmd = exec.Command("pg_dumpall", "-h", host, "-U", user, "-f", output)
		} else {
			return fmt.Errorf("binário pg_dumpall não encontrado no sistema e não pode ser executado do embutido")
		}
	} else {
		cmd = exec.Command("pg_dumpall", "-h", host, "-U", user, "-f", output)
	}

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