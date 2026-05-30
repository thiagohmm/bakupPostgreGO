package upload

import (
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
	"golang.org/x/crypto/ssh"

	"github.thiagohmm.com.br/backupPostgre/internal/config"
)

func RunSCP(ctx context.Context, cfg config.Config, archivePath string) error {
	if strings.TrimSpace(cfg.SCPDest) == "" {
		return nil
	}

	if strings.TrimSpace(cfg.SSHPassword) != "" {
		fmt.Println("Enviando via SFTP para:", cfg.SCPDest)
		return UploadViaSFTP(ctx, cfg, archivePath)
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

func ParseSCPDest(dest string, fallbackUser string) (remoteSpec, error) {
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

func UploadViaSFTP(ctx context.Context, cfg config.Config, localPath string) error {
	rs, err := ParseSCPDest(cfg.SCPDest, cfg.SSHUser)
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

type remoteSpec struct {
	User string
	Host string
	Path string
}
