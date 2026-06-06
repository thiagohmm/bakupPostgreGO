// Package backup_test contains unit tests for the pgdump package.
package backup

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireCommandOrSkip(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("comando %s não disponível no PATH deste ambiente: %v", name, err)
	}
}

// TestBackupDB tests the BackupDB function with valid parameters.
func TestBackupDB(t *testing.T) {
	requireCommandOrSkip(t, "pg_dump")

	// Arrange: Mock parameters
	host := "localhost:5432"
	db := "test_db"
	user := "test_user"
	password := "test_pass"
	output := "backup.sql"

	// Act & Assert: Call the function and verify it doesn't panic
	err := BackupDB(host, db, user, password, output)
	require.NoError(t, err, "BackupDB should not return an error with valid inputs")
}

// TestBackupDBInvalidParams tests the BackupDB function with missing parameters.
func TestBackupDBInvalidParams(t *testing.T) {
	// Arrange: Missing parameters
	host := ""
	db := ""
	user := ""
	password := ""
	output := ""

	// Act & Assert: Call the function and verify it returns an error
	err := BackupDB(host, db, user, password, output)
	require.Error(t, err, "BackupDB should return an error with missing parameters")
	require.Contains(t, err.Error(), "todos os parâmetros são obrigatórios")
}

// TestBackupAllDBs tests the BackupAllDBs function with valid parameters.
func TestBackupAllDBs(t *testing.T) {
	requireCommandOrSkip(t, "pg_dumpall")

	// Arrange: Mock parameters
	host := "localhost:5432"
	user := "test_user"
	password := "test_pass"
	output := "all_backup.sql"

	// Act & Assert: Call the function and verify it doesn't panic
	err := BackupAllDBs(host, user, password, output)
	require.NoError(t, err, "BackupAllDBs should not return an error with valid inputs")
}

// TestBackupAllDBsInvalidParams tests the BackupAllDBs function with missing parameters.
func TestBackupAllDBsInvalidParams(t *testing.T) {
	// Arrange: Missing parameters
	host := ""
	user := ""
	password := ""
	output := ""

	// Act & Assert: Call the function and verify it returns an error
	err := BackupAllDBs(host, user, password, output)
	require.Error(t, err, "BackupAllDBs should return an error with missing parameters")
	require.Contains(t, err.Error(), "todos os parâmetros são obrigatórios")
}

// TestBackupAllDBsEmptyDBs tests the BackupAllDBs function when no databases exist.
func TestBackupAllDBsEmptyDBs(t *testing.T) {
	requireCommandOrSkip(t, "pg_dumpall")

	// Arrange: Mock parameters with no databases
	host := "localhost:5432"
	user := "test_user"
	password := "test_pass"
	output := "all_backup.sql"

	// Act & Assert: Call the function and verify it handles empty database list gracefully
	err := BackupAllDBs(host, user, password, output)
	require.NoError(t, err, "BackupAllDBs should not return an error when no databases exist")
}

// TestBackupAllDBsWithEmptyOutput tests the BackupAllDBs function with empty output path.
func TestBackupAllDBsWithEmptyOutput(t *testing.T) {
	// Arrange: Empty output path
	host := "localhost:5432"
	user := "test_user"
	password := "test_pass"
	output := ""

	// Act & Assert: Call the function and verify it returns an error
	err := BackupAllDBs(host, user, password, output)
	require.Error(t, err, "BackupAllDBs should return an error with empty output")
	require.Contains(t, err.Error(), "todos os parâmetros são obrigatórios")
}
