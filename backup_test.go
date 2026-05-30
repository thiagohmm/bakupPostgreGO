package main

import (
    "bytes"
    "compress/gzip"
    "io"
    "os"
    "path/filepath"
    "testing"
)

func TestValidateConfigSingleDB_MissingHostOrUser(t *testing.T) {
    cfg := Config{PGHost: "", PGUser: ""}
    err := validateConfigSingleDB(cfg)
    if err == nil || err.Error() != "faltam flags/variáveis obrigatórias do Postgres: --pg-host/PGHOST, --pg-user/PGUSER" {
        t.Fatalf("expected missing host/user error, got %v", err)
    }
}

func TestValidateConfigSingleDB_MissingDatabase(t *testing.T) {
    cfg := Config{PGHost: "localhost", PGUser: "admin", PGDatabase: ""}
    err := validateConfigSingleDB(cfg)
    if err == nil || err.Error() != "falta database: --pg-db/PGDATABASE (para todas as databases use o comando run-all)" {
        t.Fatalf("expected missing database error, got %v", err)
    }
}

func TestValidateConfigAllDBs_Valid(t *testing.T) {
    cfg := Config{PGHost: "localhost", PGUser: "admin"}
    if err := validateConfigAllDBs(cfg); err != nil {
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
    merged := mergeWithEnvDefaults(cfg)
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

func TestGzipFile(t *testing.T) {
    // create a temporary source file
    srcPath := filepath.Join(os.TempDir(), "src.txt")
    dstPath := filepath.Join(os.TempDir(), "dst.txt.gz")
    defer os.Remove(srcPath)
    defer os.Remove(dstPath)
    content := []byte("hello world")
    if err := os.WriteFile(srcPath, content, 0o600); err != nil {
        t.Fatalf("write src failed: %v", err)
    }
    if err := gzipFile(srcPath, dstPath); err != nil {
        t.Fatalf("gzipFile failed: %v", err)
    }
    // read back and decompress
    f, err := os.Open(dstPath)
    if err != nil {
        t.Fatalf("open gzip file failed: %v", err)
    }
    gz, err := gzip.NewReader(f)
    if err != nil {
        t.Fatalf("gzip.NewReader failed: %v", err)
    }
    decoded, err := io.ReadAll(gz)
    if err != nil {
        t.Fatalf("read gzip content failed: %v", err)
    }
    if !bytes.Equal(decoded, content) {
        t.Fatalf("decompressed content mismatch, got %s, want %s", string(decoded), string(content))
    }
    gz.Close()
    f.Close()
}
