import os
import re

def main():
    with open('main.go', 'r') as f:
        main_code = f.read()

    funcs_to_extract = [
        'runBackup', 'runBackupAll', 'gzipFile', 'withOptionalPasswordEnv',
        'preparePGDumpExecutable', 'preparePGDumpAllExecutable'
    ]
    
    backup_funcs_code = []
    for func in funcs_to_extract:
        func_match = re.search(rf'func {func}\(.*?\) .*?^}}', main_code, re.DOTALL | re.MULTILINE)
        if func_match:
            backup_funcs_code.append(func_match.group(0))
            main_code = main_code.replace(func_match.group(0), '')

    os.makedirs('internal/backup', exist_ok=True)
    with open('internal/backup/runner.go', 'w') as f:
        f.write('package backup\n\n')
        f.write('import (\n\t"compress/gzip"\n\t"context"\n\t"errors"\n\t"fmt"\n\t"io"\n\t"os"\n\t"os/exec"\n\t"path/filepath"\n\t"strings"\n\t"time"\n\n\t"github.thiagohmm.com.br/backupPostgre/internal/config"\n)\n\n')
        for func_code in backup_funcs_code:
            func_code = func_code.replace('cfg Config', 'cfg config.Config')
            func_code = func_code.replace('embeddedPGDumpBytes', 'EmbeddedPGDumpBytes')
            func_code = func_code.replace('embeddedPGDumpAllBytes', 'EmbeddedPGDumpAllBytes')
            func_code = re.sub(r'^func (runBackup|runBackupAll|gzipFile|withOptionalPasswordEnv|preparePGDumpExecutable|preparePGDumpAllExecutable)', 
                               lambda m: f'func {m.group(1)[0].upper() + m.group(1)[1:]}', 
                               func_code, flags=re.MULTILINE)
            # update internal calls
            func_code = func_code.replace('withOptionalPasswordEnv(', 'WithOptionalPasswordEnv(')
            func_code = func_code.replace('gzipFile(', 'GzipFile(')
            f.write(func_code + '\n\n')

    main_code = main_code.replace('runBackup(', 'backup.RunBackup(')
    main_code = main_code.replace('runBackupAll(', 'backup.RunBackupAll(')
    main_code = main_code.replace('preparePGDumpExecutable(', 'backup.PreparePGDumpExecutable(')
    main_code = main_code.replace('preparePGDumpAllExecutable(', 'backup.PreparePGDumpAllExecutable(')
    
    # "backup" is already imported from earlier fixes
    if '"github.thiagohmm.com.br/backupPostgre/internal/backup"' not in main_code:
        main_code = main_code.replace('import (', 'import (\n\t"github.thiagohmm.com.br/backupPostgre/internal/backup"\n')

    with open('main.go', 'w') as f:
        f.write(main_code)

if __name__ == '__main__':
    main()
