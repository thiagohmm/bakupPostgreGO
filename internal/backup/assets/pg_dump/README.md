Coloque aqui o binário do `pg_dump` para embutir no executável.

### Como preparar

- **Caminho esperado**: `assets/pg_dump/pg_dump`
- O binário precisa ser compatível com o **mesmo SO/arquitetura** do executável (ex: Linux x86_64).

### Como compilar com pg_dump embutido

```bash
go build -tags pg_dump_embedded -o backup-postgres
```

Depois disso, o comando `backup-postgres run ...` **não precisa** que `pg_dump` esteja instalado no sistema.
