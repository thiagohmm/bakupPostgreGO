package config

import (
	"os"
	"testing"
)

func TestValidateConfigSingleDB_MissingHostOrUser(t *testing.T) {
    cfg := Config{PGHost: "", PGUser: ""}
    err := ValidateConfigSingleDB(cfg)
    if err == nil || err.Error() != "faltam flags/variáveis obrigatórias do Postgres: --pg-host/PGHOST, --pg-user/PGUSER" {
        t.Fatalf("expected missing host/user error, got %v", err)
    }
}

func TestValidateConfigSingleDB_MissingDatabase(t *testing.T) {
    cfg := Config{PGHost: "localhost", PGUser: "admin", PGDatabase: ""}
    err := ValidateConfigSingleDB(cfg)
    if err == nil || err.Error() != "falta database: --pg-db/PGDATABASE (para todas as databases use o comando run-all)" {
        t.Fatalf("expected missing database error, got %v", err)
    }
}

func TestValidateConfigAllDBs_Valid(t *testing.T) {
    cfg := Config{PGHost: "localhost", PGUser: "admin"}
    if err := ValidateConfigAllDBs(cfg); err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
}

func TestMergeWithEnvDefaults(t *testing.T) {
    // set environment variables
    os.Setenv("PGHOST", "envhost")
    os.Setenv("PGUSER", "envuser")
    os.Setenv("PGPORT", "5433")
    os.Setenv("COMPRESS", "none")
    os.Setenv("BACKUP_DIR", "/tmp/backups")
    defer func() {
        os.Unsetenv("PGHOST")
        os.Unsetenv("PGUSER")
        os.Unsetenv("PGPORT")
        os.Unsetenv("COMPRESS")
        os.Unsetenv("BACKUP_DIR")
    }()

    cfg := Config{}
    merged := MergeWithEnvDefaults(cfg)
    if merged.PGHost != "envhost" || merged.PGUser != "envuser" {
        t.Fatalf("expected env values for host/user, got %s/%s", merged.PGHost, merged.PGUser)
    }
    if merged.PGPort != "5433" {
        t.Fatalf("expected PGPORT env override, got %s", merged.PGPort)
    }
    if merged.Compress != "none" {
        t.Fatalf("expected COMPRESS env override, got %s", merged.Compress)
    }
    if merged.BackupDir != "/tmp/backups" {
        t.Fatalf("expected BACKUP_DIR env override, got %s", merged.BackupDir)
    }
}
