package config

import (
    "errors"
    "fmt"
    "os"
    "strconv"
    "strings"
)

type Config struct {
    EnvFile string

    PGDumpPath string
    PGDumpAllPath string

    PGHost     string
    PGPort     string
    PGDatabase string
    PGUser     string
    PGPassword string

    BackupDir    string
    BackupPrefix string
    Compress     string // gzip|none

    SCPDest         string
    SCPPort         int
    SCPIdentityFile string

    SSHUser     string
    SSHPassword string
}

func MergeWithEnvDefaults(cfg Config) Config {
    get := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }

    if cfg.PGDumpPath == "" {
        cfg.PGDumpPath = get("PG_DUMP_PATH")
    }
    if cfg.PGDumpAllPath == "" {
        cfg.PGDumpAllPath = get("PG_DUMPALL_PATH")
    }
    if cfg.PGHost == "" {
        cfg.PGHost = get("PGHOST")
    }
    if cfg.PGPort == "" {
        cfg.PGPort = firstNonEmpty(get("PGPORT"), "5432")
    }
    if cfg.PGDatabase == "" {
        cfg.PGDatabase = get("PGDATABASE")
    }
    if cfg.PGUser == "" {
        cfg.PGUser = get("PGUSER")
    }
    if cfg.PGPassword == "" {
        cfg.PGPassword = get("PGPASSWORD")
    }
    if cfg.BackupDir == "" {
        cfg.BackupDir = firstNonEmpty(get("BACKUP_DIR"), "./backups")
    }
    if cfg.BackupPrefix == "" {
        cfg.BackupPrefix = firstNonEmpty(get("BACKUP_PREFIX"), "pg_backup")
    }
    if cfg.Compress == "" {
        cfg.Compress = firstNonEmpty(get("COMPRESS"), "gzip")
    }
    if cfg.SCPDest == "" {
        cfg.SCPDest = get("SCP_DEST")
    }
    if cfg.SCPPort == 0 {
        p, err := strconv.Atoi(firstNonEmpty(get("SCP_PORT"), "22"))
        if err == nil {
            cfg.SCPPort = p
        }
    }
    if cfg.SCPIdentityFile == "" {
        cfg.SCPIdentityFile = get("SCP_IDENTITY_FILE")
    }
    if cfg.SSHUser == "" {
        cfg.SSHUser = get("SSH_USER")
    }
    if cfg.SSHPassword == "" {
        cfg.SSHPassword = get("SSH_PASSWORD")
    }

    return cfg
}

func validateCommon(cfg Config) error {
    if cfg.PGHost == "" || cfg.PGUser == "" {
        return errors.New("faltam flags/variáveis obrigatórias do Postgres: --pg-host/PGHOST, --pg-user/PGUSER")
    }
    if cfg.PGPort == "" {
        cfg.PGPort = "5432"
    }
    if cfg.BackupDir == "" {
        cfg.BackupDir = "./backups"
    }
    if cfg.BackupPrefix == "" {
        cfg.BackupPrefix = "pg_backup"
    }
    if cfg.Compress == "" {
        cfg.Compress = "gzip"
    }
    if cfg.SCPPort <= 0 {
        cfg.SCPPort = 22
    }
    switch cfg.Compress {
    case "gzip", "none":
    default:
        return fmt.Errorf("compress inválido: %q (use gzip|none)", cfg.Compress)
    }
    return nil
}

func ValidateConfigSingleDB(cfg Config) error {
    if err := validateCommon(cfg); err != nil {
        return err
    }
    if strings.TrimSpace(cfg.PGDatabase) == "" {
        return errors.New("falta database: --pg-db/PGDATABASE (para todas as databases use o comando run-all)")
    }
    return nil
}

func ValidateConfigAllDBs(cfg Config) error {
    if err := validateCommon(cfg); err != nil {
        return err
    }
    return nil
}

// Helper function used by mergeWithEnvDefaults
func firstNonEmpty(v, fallback string) string {
    if strings.TrimSpace(v) == "" {
        return fallback
    }
    return v
}
