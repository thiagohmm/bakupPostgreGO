package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh"
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

func main() {
	cfg := Config{}
	root := buildRootCmd(&cfg)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func buildRootCmd(cfg *Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup-postgres",
		Short: "Backup SQL do PostgreSQL e envio via scp",
		Long: `Gera backup SQL do PostgreSQL e envia via scp.

Você pode passar parâmetros por flags ou por variáveis de ambiente (valores de flags têm prioridade).
Opcionalmente, use --env para carregar um arquivo .env.`,
		SilenceUsage: true,
	}

	cmd.PersistentFlags().StringVar(&cfg.EnvFile, "env", "", "caminho para arquivo .env (opcional)")

	cmd.AddCommand(buildRunCmd(cfg))
	cmd.AddCommand(buildRunAllCmd(cfg))
	return cmd
}

func buildRunCmd(cfg *Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Backup de uma database (pg_dump) e envia via scp",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.EnvFile != "" {
				if err := loadDotEnv(cfg.EnvFile); err != nil {
					return fmt.Errorf("falha ao carregar --env: %w", err)
				}
			}

			merged := mergeWithEnvDefaults(*cfg)
			if err := validateConfigSingleDB(merged); err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Hour)
			defer cancel()

			pgDumpExec, cleanup, err := preparePGDumpExecutable(merged)
			if err != nil {
				return err
			}
			if cleanup != nil {
				defer cleanup()
			}

			archivePath, err := runBackup(ctx, merged, pgDumpExec)
			if err != nil {
				return fmt.Errorf("backup falhou: %w", err)
			}

			if strings.TrimSpace(merged.SCPDest) != "" {
				if err := requireCmd("scp"); err != nil {
					return err
				}
				if err := runSCP(ctx, merged, archivePath); err != nil {
					return fmt.Errorf("scp falhou: %w", err)
				}
			} else {
				fmt.Println("Backup gerado localmente (scp não configurado):", archivePath)
			}

			fmt.Println("OK.")
			return nil
		},
	}

	cmd.Flags().StringVar(&cfg.PGDumpPath, "pg-dump", "", "caminho do pg_dump (env: PG_DUMP_PATH). Se vazio, usa embutido (se compilado) ou PATH")

	// Postgres
	cmd.Flags().StringVar(&cfg.PGHost, "pg-host", "", "host do PostgreSQL (env: PGHOST)")
	cmd.Flags().StringVar(&cfg.PGPort, "pg-port", "", "porta do PostgreSQL (env: PGPORT, default: 5432)")
	cmd.Flags().StringVar(&cfg.PGDatabase, "pg-db", "", "nome do banco (env: PGDATABASE)")
	cmd.Flags().StringVar(&cfg.PGUser, "pg-user", "", "usuário do PostgreSQL (env: PGUSER)")
	cmd.Flags().StringVar(&cfg.PGPassword, "pg-pass", "", "senha do PostgreSQL (env: PGPASSWORD)")

	// Backup
	cmd.Flags().StringVar(&cfg.BackupDir, "backup-dir", "", "diretório de saída (env: BACKUP_DIR, default: ./backups)")
	cmd.Flags().StringVar(&cfg.BackupPrefix, "backup-prefix", "", "prefixo do arquivo (env: BACKUP_PREFIX, default: pg_backup)")
	cmd.Flags().StringVar(&cfg.Compress, "compress", "", "compressão: gzip|none (env: COMPRESS, default: gzip)")

	// SCP/SSH
	cmd.Flags().StringVar(&cfg.SCPDest, "scp-dest", "", "destino scp (env: SCP_DEST) ex: user@host:/path/")
	cmd.Flags().IntVar(&cfg.SCPPort, "scp-port", 0, "porta do ssh/scp (env: SCP_PORT, default: 22)")
	cmd.Flags().StringVar(&cfg.SCPIdentityFile, "scp-identity", "", "arquivo de identidade ssh (env: SCP_IDENTITY_FILE)")
	cmd.Flags().StringVar(&cfg.SSHUser, "ssh-user", "", "usuário SSH (env: SSH_USER). Se SCP_DEST já tiver user@host, esse valor é ignorado")
	cmd.Flags().StringVar(&cfg.SSHPassword, "ssh-pass", "", "senha SSH para envio (env: SSH_PASSWORD). Se setado, usa SFTP em Go (não-interativo)")

	return cmd
}

func buildRunAllCmd(cfg *Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run-all",
		Short: "Backup de todas as databases (pg_dumpall) e envia via scp",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.EnvFile != "" {
				if err := loadDotEnv(cfg.EnvFile); err != nil {
					return fmt.Errorf("falha ao carregar --env: %w", err)
				}
			}

			merged := mergeWithEnvDefaults(*cfg)
			if err := validateConfigAllDBs(merged); err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Hour)
			defer cancel()

			pgDumpAllExec, cleanup, err := preparePGDumpAllExecutable(merged)
			if err != nil {
				return err
			}
			if cleanup != nil {
				defer cleanup()
			}

			archivePath, err := runBackupAll(ctx, merged, pgDumpAllExec)
			if err != nil {
				return fmt.Errorf("backup (all) falhou: %w", err)
			}

			if strings.TrimSpace(merged.SCPDest) != "" {
				if err := requireCmd("scp"); err != nil {
					return err
				}
				if err := runSCP(ctx, merged, archivePath); err != nil {
					return fmt.Errorf("scp falhou: %w", err)
				}
			} else {
				fmt.Println("Backup gerado localmente (scp não configurado):", archivePath)
			}

			fmt.Println("OK.")
			return nil
		},
	}

	cmd.Flags().StringVar(&cfg.PGDumpAllPath, "pg-dumpall", "", "caminho do pg_dumpall (env: PG_DUMPALL_PATH). Se vazio, usa embutido (se compilado) ou PATH")

	// Postgres (para pg_dumpall não precisa de --pg-db)
	cmd.Flags().StringVar(&cfg.PGHost, "pg-host", "", "host do PostgreSQL (env: PGHOST)")
	cmd.Flags().StringVar(&cfg.PGPort, "pg-port", "", "porta do PostgreSQL (env: PGPORT, default: 5432)")
	cmd.Flags().StringVar(&cfg.PGUser, "pg-user", "", "usuário do PostgreSQL (env: PGUSER)")
	cmd.Flags().StringVar(&cfg.PGPassword, "pg-pass", "", "senha do PostgreSQL (env: PGPASSWORD)")

	// Backup
	cmd.Flags().StringVar(&cfg.BackupDir, "backup-dir", "", "diretório de saída (env: BACKUP_DIR, default: ./backups)")
	cmd.Flags().StringVar(&cfg.BackupPrefix, "backup-prefix", "", "prefixo do arquivo (env: BACKUP_PREFIX, default: pg_backup)")
	cmd.Flags().StringVar(&cfg.Compress, "compress", "", "compressão: gzip|none (env: COMPRESS, default: gzip)")

	// SCP/SSH
	cmd.Flags().StringVar(&cfg.SCPDest, "scp-dest", "", "destino scp (env: SCP_DEST) ex: user@host:/path/")
	cmd.Flags().IntVar(&cfg.SCPPort, "scp-port", 0, "porta do ssh/scp (env: SCP_PORT, default: 22)")
	cmd.Flags().StringVar(&cfg.SCPIdentityFile, "scp-identity", "", "arquivo de identidade ssh (env: SCP_IDENTITY_FILE)")
	cmd.Flags().StringVar(&cfg.SSHUser, "ssh-user", "", "usuário SSH (env: SSH_USER). Se SCP_DEST já tiver user@host, esse valor é ignorado")
	cmd.Flags().StringVar(&cfg.SSHPassword, "ssh-pass", "", "senha SSH para envio (env: SSH_PASSWORD). Se setado, usa SFTP em Go (não-interativo)")

	return cmd
}

func mergeWithEnvDefaults(cfg Config) Config {
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

func validateConfigSingleDB(cfg Config) error {
	if err := validateCommon(cfg); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.PGDatabase) == "" {
		return errors.New("falta database: --pg-db/PGDATABASE (para todas as databases use o comando run-all)")
	}
	return nil
}

func validateConfigAllDBs(cfg Config) error {
	if err := validateCommon(cfg); err != nil {
		return err
	}
	return nil
}

func runBackup(ctx context.Context, cfg Config, pgDumpExec string) (string, error) {
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
	cmd.Env = append(os.Environ(), withOptionalPasswordEnv(cfg.PGPassword)...)

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

		if err := gzipFile(dumpPath, gzPath); err != nil {
			return "", err
		}
		_ = os.Remove(dumpPath)
		return gzPath, nil
	default:
		return "", fmt.Errorf("COMPRESS inválido: %q", cfg.Compress)
	}
}

func runBackupAll(ctx context.Context, cfg Config, pgDumpAllExec string) (string, error) {
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
	cmd.Env = append(os.Environ(), withOptionalPasswordEnv(cfg.PGPassword)...)

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
		if err := gzipFile(dumpPath, gzPath); err != nil {
			return "", err
		}
		_ = os.Remove(dumpPath)
		return gzPath, nil
	default:
		return "", fmt.Errorf("COMPRESS inválido: %q", cfg.Compress)
	}
}

func runSCP(ctx context.Context, cfg Config, archivePath string) error {
	if strings.TrimSpace(cfg.SCPDest) == "" {
		return nil
	}

	if strings.TrimSpace(cfg.SSHPassword) != "" {
		fmt.Println("Enviando via SFTP para:", cfg.SCPDest)
		return uploadViaSFTP(ctx, cfg, archivePath)
	}

	fmt.Println("Enviando via scp para:", cfg.SCPDest)
	args := []string{"-P", strconv.Itoa(cfg.SCPPort)}
	if cfg.SCPIdentityFile != "" {
		args = append(args, "-i", cfg.SCPIdentityFile)
	}
	args = append(args, archivePath, cfg.SCPDest)

	cmd := exec.CommandContext(ctx, "scp", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type remoteSpec struct {
	User string
	Host string
	Path string
}

func parseSCPDest(dest string, fallbackUser string) (remoteSpec, error) {
	// Esperado: [user@]host:/caminho/arquivo-ou-pasta
	left, right, ok := strings.Cut(dest, ":")
	if !ok || strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return remoteSpec{}, fmt.Errorf("SCP_DEST inválido: %q (use user@host:/caminho/)", dest)
	}
	userHost := strings.TrimSpace(left)
	remotePath := strings.TrimSpace(right)

	var user, host string
	if u, h, hasAt := strings.Cut(userHost, "@"); hasAt {
		user = strings.TrimSpace(u)
		host = strings.TrimSpace(h)
	} else {
		host = strings.TrimSpace(userHost)
		user = strings.TrimSpace(fallbackUser)
	}

	if user == "" {
		return remoteSpec{}, errors.New("usuário SSH não informado. Use user@host:/path em SCP_DEST ou --ssh-user/SSH_USER")
	}
	if host == "" {
		return remoteSpec{}, errors.New("host inválido em SCP_DEST")
	}
	if !strings.HasPrefix(remotePath, "/") {
		// scp aceita relativo, mas aqui mantemos consistente.
		// Se precisar relativo, remova essa restrição.
	}

	return remoteSpec{User: user, Host: host, Path: remotePath}, nil
}

func uploadViaSFTP(ctx context.Context, cfg Config, localPath string) error {
	rs, err := parseSCPDest(cfg.SCPDest, cfg.SSHUser)
	if err != nil {
		return err
	}

	auths := []ssh.AuthMethod{
		ssh.Password(cfg.SSHPassword),
	}
	if strings.TrimSpace(cfg.SCPIdentityFile) != "" {
		key, readErr := os.ReadFile(cfg.SCPIdentityFile)
		if readErr != nil {
			return fmt.Errorf("falha ao ler chave SSH (%s): %w", cfg.SCPIdentityFile, readErr)
		}
		signer, parseErr := ssh.ParsePrivateKey(key)
		if parseErr != nil {
			return fmt.Errorf("falha ao parsear chave SSH (%s): %w", cfg.SCPIdentityFile, parseErr)
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}

	sshCfg := &ssh.ClientConfig{
		User:            rs.User,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}

	addr := net.JoinHostPort(rs.Host, strconv.Itoa(cfg.SCPPort))
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	c, chans, reqs, err := ssh.NewClientConn(conn, addr, sshCfg)
	if err != nil {
		return err
	}
	client := ssh.NewClient(c, chans, reqs)
	defer client.Close()

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	src, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dstPath := rs.Path
	if strings.HasSuffix(dstPath, "/") {
		dstPath = dstPath + filepath.Base(localPath)
	}

	dst, err := sftpClient.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Close()
}

func gzipFile(srcPath, dstPath string) error {
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

func withOptionalPasswordEnv(pw string) []string {
	if strings.TrimSpace(pw) == "" {
		return nil
	}
	return []string{"PGPASSWORD=" + pw}
}

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			continue
		}
		v = strings.Trim(v, `"'`)
		if err := os.Setenv(k, v); err != nil {
			return err
		}
	}
	return sc.Err()
}

func requireCmd(name string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("comando %q não encontrado no PATH", name)
	}
	return nil
}

func preparePGDumpExecutable(cfg Config) (path string, cleanup func(), err error) {
	// 1) Se usuário informou um caminho, valida e usa.
	if strings.TrimSpace(cfg.PGDumpPath) != "" {
		if _, statErr := os.Stat(cfg.PGDumpPath); statErr != nil {
			return "", nil, fmt.Errorf("pg_dump não encontrado em --pg-dump: %w", statErr)
		}
		return cfg.PGDumpPath, nil, nil
	}

	// 2) Se o binário estiver embutido (build tag), extrai e usa.
	if b, ok := embeddedPGDumpBytes(); ok {
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

func preparePGDumpAllExecutable(cfg Config) (path string, cleanup func(), err error) {
	if strings.TrimSpace(cfg.PGDumpAllPath) != "" {
		if _, statErr := os.Stat(cfg.PGDumpAllPath); statErr != nil {
			return "", nil, fmt.Errorf("pg_dumpall não encontrado em --pg-dumpall: %w", statErr)
		}
		return cfg.PGDumpAllPath, nil, nil
	}

	if b, ok := embeddedPGDumpAllBytes(); ok {
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

func firstNonEmpty(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

