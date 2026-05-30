import os

def fix_backup_exports():
    for f in os.listdir('internal/backup'):
        if not f.endswith('.go'): continue
        path = os.path.join('internal/backup', f)
        with open(path, 'r') as file:
            content = file.read()
        content = content.replace('func embeddedPGDumpBytes', 'func EmbeddedPGDumpBytes')
        content = content.replace('func embeddedPGDumpAllBytes', 'func EmbeddedPGDumpAllBytes')
        with open(path, 'w') as file:
            file.write(content)

def fix_main():
    with open('main.go', 'r') as file:
        content = file.read()
    
    # Replace calls
    content = content.replace('embeddedPGDumpBytes()', 'backup.EmbeddedPGDumpBytes()')
    content = content.replace('embeddedPGDumpAllBytes()', 'backup.EmbeddedPGDumpAllBytes()')
    
    # Add import
    import_statement = '"github.thiagohmm.com.br/backupPostgre/internal/backup"'
    if import_statement not in content:
        content = content.replace('import (', 'import (\n\t' + import_statement + '\n')
        
    with open('main.go', 'w') as file:
        file.write(content)

if __name__ == '__main__':
    fix_backup_exports()
    fix_main()
