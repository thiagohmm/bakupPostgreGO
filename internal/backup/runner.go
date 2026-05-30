package backup

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.thiagohmm.com.br/backupPostgre/internal/config"
)

func RunBackup(ctx context.Context, cfg config.Config, pgDumpExec string) (string, error) {
	if err := os.MkdirAll(cfg.BackupDir, 0o755); err != nil {
		return "", err
	}

	ts := time.Now().Format("20060102_150405")
	baseName := fmt.Sprintf("%s_%s_%s.sql", cfg.BackupPrefix, cfg.PGDatabase, ts)
	dumpPath := filepath.Join(cfg.BackupDir, baseName)

	fmt.Println("Gerando backup SQL em:", dumpPath)

	outFile, err := os.Create(dumpPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	cmd := exec.CommandContext(ctx, pgDumpExec,
		"--host="+cfg.PGHost,
		"--port="+cfg.PGPort,
		"--username="+cfg.PGUser,
		"--format=plain",
		"--encoding=UTF8",
		"--no-owner",
		"--no-privileges",
		"--verbose",
		cfg.PGDatabase,
	)
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), WithOptionalPasswordEnv(cfg.PGPassword)...)

	if err := cmd.Run(); err != nil {
		return "", err
	}

	if err := outFile.Close(); err != nil {
		return "", err
	}

	switch cfg.Compress {
	case "none":
		return dumpPath, nil
	case "gzip":
		gzPath := dumpPath + ".gz"
		fmt.Println("Comprimindo com gzip:", gzPath)

		if err := GzipFile(dumpPath, gzPath); err != nil {
			return "", err
		}
		_ = os.Remove(dumpPath)
		return gzPath, nil
	default:
		return "", fmt.Errorf("COMPRESS inválido: %q", cfg.Compress)
	}
}

func RunBackupAll(ctx context.Context, cfg config.Config, pgDumpAllExec string) (string, error) {
	if err := os.MkdirAll(cfg.BackupDir, 0o755); err != nil {
		return "", err
	}

	ts := time.Now().Format("20060102_150405")
	baseName := fmt.Sprintf("%s_all_%s.sql", cfg.BackupPrefix, ts)
	dumpPath := filepath.Join(cfg.BackupDir, baseName)

	fmt.Println("Gerando backup SQL (todas as databases) em:", dumpPath)

	outFile, err := os.Create(dumpPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	cmd := exec.CommandContext(ctx, pgDumpAllExec,
		"--host="+cfg.PGHost,
		"--port="+cfg.PGPort,
		"--username="+cfg.PGUser,
		"--verbose",
	)
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), WithOptionalPasswordEnv(cfg.PGPassword)...)

	if err := cmd.Run(); err != nil {
		return "", err
	}

	if err := outFile.Close(); err != nil {
		return "", err
	}

	switch cfg.Compress {
	case "none":
		return dumpPath, nil
	case "gzip":
		gzPath := dumpPath + ".gz"
		fmt.Println("Comprimindo com gzip:", gzPath)
		if err := GzipFile(dumpPath, gzPath); err != nil {
			return "", err
		}
		_ = os.Remove(dumpPath)
		return gzPath, nil
	default:
		return "", fmt.Errorf("COMPRESS inválido: %q", cfg.Compress)
	}
}

func GzipFile(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()

	gw := gzip.NewWriter(out)
	defer gw.Close()

	if _, err := io.Copy(gw, in); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	return out.Close()
}

func WithOptionalPasswordEnv(pw string) []string {
	if strings.TrimSpace(pw) == "" {
		return nil
	}
	return []string{"PGPASSWORD=" + pw}
}

func PreparePGDumpExecutable(cfg config.Config) (path string, cleanup func(), err error) {
	// 1) Se usuário informou um caminho, valida e usa.
	if strings.TrimSpace(cfg.PGDumpPath) != "" {
		if _, statErr := os.Stat(cfg.PGDumpPath); statErr != nil {
			return "", nil, fmt.Errorf("pg_dump não encontrado em --pg-dump: %w", statErr)
		}
		return cfg.PGDumpPath, nil, nil
	}

	// 2) Se o binário estiver embutido (build tag), extrai e usa.
	if b, ok := EmbeddedPGDumpBytes(); ok {
		tmpDir, mkErr := os.MkdirTemp("", "backup-postgres-")
		if mkErr != nil {
			return "", nil, mkErr
		}
		execPath := filepath.Join(tmpDir, "pg_dump")
		if writeErr := os.WriteFile(execPath, b, 0o700); writeErr != nil {
			_ = os.RemoveAll(tmpDir)
			return "", nil, writeErr
		}
		cleanupFn := func() { _ = os.RemoveAll(tmpDir) }
		return execPath, cleanupFn, nil
	}

	// 3) Fallback: procura no PATH.
	p, lookErr := exec.LookPath("pg_dump")
	if lookErr != nil {
		return "", nil, errors.New("pg_dump não está no PATH e não foi embutido. Instale o postgresql-client, use --pg-dump, ou compile com -tags pg_dump_embedded")
	}
	return p, nil, nil
}

func PreparePGDumpAllExecutable(cfg config.Config) (path string, cleanup func(), err error) {
	if strings.TrimSpace(cfg.PGDumpAllPath) != "" {
		if _, statErr := os.Stat(cfg.PGDumpAllPath); statErr != nil {
			return "", nil, fmt.Errorf("pg_dumpall não encontrado em --pg-dumpall: %w", statErr)
		}
		return cfg.PGDumpAllPath, nil, nil
	}

	if b, ok := EmbeddedPGDumpAllBytes(); ok {
		tmpDir, mkErr := os.MkdirTemp("", "backup-postgres-")
		if mkErr != nil {
			return "", nil, mkErr
		}
		execPath := filepath.Join(tmpDir, "pg_dumpall")
		if writeErr := os.WriteFile(execPath, b, 0o700); writeErr != nil {
			_ = os.RemoveAll(tmpDir)
			return "", nil, writeErr
		}
		cleanupFn := func() { _ = os.RemoveAll(tmpDir) }
		return execPath, cleanupFn, nil
	}

	p, lookErr := exec.LookPath("pg_dumpall")
	if lookErr != nil {
		return "", nil, errors.New("pg_dumpall não está no PATH e não foi embutido. Instale o postgresql-client, use --pg-dumpall, ou compile com -tags pg_dump_embedded (incluindo pg_dumpall)")
	}
	return p, nil, nil
}

