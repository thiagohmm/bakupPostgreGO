import os
import re

def main():
    with open('main.go', 'r') as f:
        main_code = f.read()

    # Extract Config struct
    config_struct_match = re.search(r'type Config struct \{.*?\n\}', main_code, re.DOTALL)
    if not config_struct_match: return
    config_struct = config_struct_match.group(0)

    # Extract functions for config
    funcs_to_extract = [
        'mergeWithEnvDefaults', 'validateCommon', 'validateConfigSingleDB',
        'validateConfigAllDBs', 'loadDotEnv', 'firstNonEmpty'
    ]
    
    config_funcs_code = []
    for func in funcs_to_extract:
        # Regex to match func body (assuming simple non-nested closing braces at column 0)
        func_match = re.search(rf'func {func}\(.*?\) .*?^}}', main_code, re.DOTALL | re.MULTILINE)
        if func_match:
            config_funcs_code.append(func_match.group(0))
            # Remove from main.go
            main_code = main_code.replace(func_match.group(0), '')

    main_code = main_code.replace(config_struct, '')

    # Create internal/config/config.go
    os.makedirs('internal/config', exist_ok=True)
    with open('internal/config/config.go', 'w') as f:
        f.write('package config\n\n')
        f.write('import (\n\t"bufio"\n\t"errors"\n\t"fmt"\n\t"os"\n\t"strconv"\n\t"strings"\n)\n\n')
        f.write(config_struct.replace('type Config', 'type Config') + '\n\n')
        for func_code in config_funcs_code:
            # Export functions
            func_code = re.sub(r'^func (mergeWithEnvDefaults|validateCommon|validateConfigSingleDB|validateConfigAllDBs|loadDotEnv|firstNonEmpty)', 
                               lambda m: f'func {m.group(1)[0].upper() + m.group(1)[1:]}', 
                               func_code, flags=re.MULTILINE)
            func_code = func_code.replace('firstNonEmpty(', 'FirstNonEmpty(')
            f.write(func_code + '\n\n')

    # Update main.go references to config
    main_code = main_code.replace('cfg := Config{}', 'cfg := config.Config{}')
    main_code = main_code.replace('*Config', '*config.Config')
    main_code = main_code.replace('cfg Config', 'cfg config.Config')
    main_code = main_code.replace('mergeWithEnvDefaults', 'config.MergeWithEnvDefaults')
    main_code = main_code.replace('validateConfigSingleDB', 'config.ValidateConfigSingleDB')
    main_code = main_code.replace('validateConfigAllDBs', 'config.ValidateConfigAllDBs')
    main_code = main_code.replace('loadDotEnv', 'config.LoadDotEnv')

    # Add config import if not present
    if '"github.thiagohmm.com.br/backupPostgre/internal/config"' not in main_code:
        main_code = main_code.replace('import (', 'import (\n\t"github.thiagohmm.com.br/backupPostgre/internal/config"\n')

    with open('main.go', 'w') as f:
        f.write(main_code)

    print("Config extraction complete.")

if __name__ == '__main__':
    main()
