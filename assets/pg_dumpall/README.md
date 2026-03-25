Coloque aqui o binário do `pg_dumpall` para embutir no executável.

### Caminho esperado

- `assets/pg_dumpall/pg_dumpall`

### Build com embed

```bash
go build -tags pg_dump_embedded -o backup-postgres
```

Você precisará colocar **os dois binários**:
- `assets/pg_dump/pg_dump`
- `assets/pg_dumpall/pg_dumpall`
