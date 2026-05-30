# Plano de Clean Code para o Projeto de Backup de PostgreSQL

## Fase 1: Organização e Estrutura de Pacotes
- Criar estrutura de pacotes:
  - `internal/db` → operações com banco de dados.
  - `internal/backup` → lógica de backup.
  - `internal/config` → configuração do sistema.
- Renomear arquivos:
  - `pgdump_embedded.go` → `internal/backup/pgdump_embedded.go`
  - `pgdump_noembed.go` → `internal/backup/pgdump_noembed.go`
  - `pgdumpall_embedded.go` → `internal/backup/pgdumpall_embedded.go`

## Fase 2: Refatorar Arquivos de Backup
- Unificar lógica entre `pgdump_embedded` e `pgdump_noembed`.
- Criar um único arquivo `internal/backup/pgdump.go` com funções genéricas.
- Adicionar validações de parâmetros (ex: verificar se o caminho do arquivo é válido).

## Fase 3: Adicionar Documentação
- Adicionar comentários de doc para todas as funções principais.
- Exemplo:
  ```go
  // Backup do banco de dados PostgreSQL usando pg_dump
  // Parâmetros: host, database, user, password, output file
  // Retorna erro em caso de falha
  func BackupDB(host, db, user, password, output string) error {
  ```

## Fase 4: Aplicar Princípios de Clean Code
- Evitar funções longas (>15 linhas).
- Usar nomes de variáveis descritivos.
- Separar responsabilidades (ex: configuração, execução, logs).

## Fase 5: Testes
- Criar testes unitários para:
  - Backup com sucesso.
  - Backup com erro (ex: conexão falha).
- Usar `testing` e `testify` para validação.

## Fase 6: Documentação e README
- Criar `README.md` com:
  - Descrição do projeto.
  - Como usar.
  - Exemplos de chamadas.
  - Dependências e configuração.