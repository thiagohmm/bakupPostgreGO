package main

import (
	"github.thiagohmm.com.br/backupPostgre/internal/upload"

	"github.thiagohmm.com.br/backupPostgre/internal/config"

	"github.thiagohmm.com.br/backupPostgre/internal/backup"

	"context"
	"fmt"
	"os"
	"time"
	"os/exec"
"strings"
	"github.com/spf13/cobra"
	)

func main() {
	cfg := config.Config{}
	root := buildRootCmd(&cfg)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func buildRootCmd(cfg *config.Config) *cobra.Command {
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

func buildRunCmd(cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Backup de uma database (pg_dump) e envia via scp",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.EnvFile != "" {
				if err := config.LoadDotEnv(cfg.EnvFile); err != nil {
					return fmt.Errorf("falha ao carregar --env: %w", err)
				}
			}

			merged := config.MergeWithEnvDefaults(*cfg)
			if err := config.ValidateConfigSingleDB(merged); err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Hour)
			defer cancel()

			pgDumpExec, cleanup, err := backup.PreparePGDumpExecutable(merged)
			if err != nil {
				return err
			}
			if cleanup != nil {
				defer cleanup()
			}

			archivePath, err := backup.RunBackup(ctx, merged, pgDumpExec)
			if err != nil {
				return fmt.Errorf("backup falhou: %w", err)
			}

			if strings.TrimSpace(merged.SCPDest) != "" {
				if err := requireCmd("scp"); err != nil {
					return err
				}
				if err := upload.RunSCP(ctx, merged, archivePath); err != nil {
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

func buildRunAllCmd(cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run-all",
		Short: "Backup de todas as databases (pg_dumpall) e envia via scp",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.EnvFile != "" {
				if err := config.LoadDotEnv(cfg.EnvFile); err != nil {
					return fmt.Errorf("falha ao carregar --env: %w", err)
				}
			}

			merged := config.MergeWithEnvDefaults(*cfg)
			if err := config.ValidateConfigAllDBs(merged); err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Hour)
			defer cancel()

			pgDumpAllExec, cleanup, err := backup.PreparePGDumpAllExecutable(merged)
			if err != nil {
				return err
			}
			if cleanup != nil {
				defer cleanup()
			}

			archivePath, err := backup.RunBackupAll(ctx, merged, pgDumpAllExec)
			if err != nil {
				return fmt.Errorf("backup (all) falhou: %w", err)
			}

			if strings.TrimSpace(merged.SCPDest) != "" {
				if err := requireCmd("scp"); err != nil {
					return err
				}
				if err := upload.RunSCP(ctx, merged, archivePath); err != nil {
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









func requireCmd(name string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("comando %q não encontrado no PATH", name)
	}
	return nil
}





func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
