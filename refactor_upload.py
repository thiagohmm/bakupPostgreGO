import os
import re

def main():
    with open('main.go', 'r') as f:
        main_code = f.read()

    funcs_to_extract = [
        'runSCP', 'parseSCPDest', 'uploadViaSFTP'
    ]
    
    upload_funcs_code = []
    for func in funcs_to_extract:
        func_match = re.search(rf'func {func}\(.*?\) .*?^}}', main_code, re.DOTALL | re.MULTILINE)
        if func_match:
            upload_funcs_code.append(func_match.group(0))
            main_code = main_code.replace(func_match.group(0), '')

    remote_spec_match = re.search(r'type remoteSpec struct \{.*?\n\}', main_code, re.DOTALL)
    if remote_spec_match:
        upload_funcs_code.append(remote_spec_match.group(0))
        main_code = main_code.replace(remote_spec_match.group(0), '')

    os.makedirs('internal/upload', exist_ok=True)
    with open('internal/upload/upload.go', 'w') as f:
        f.write('package upload\n\n')
        f.write('import (\n\t"context"\n\t"errors"\n\t"fmt"\n\t"io"\n\t"net"\n\t"os"\n\t"os/exec"\n\t"path/filepath"\n\t"strconv"\n\t"strings"\n\t"time"\n\n\t"github.com/pkg/sftp"\n\t"golang.org/x/crypto/ssh"\n\n\t"github.thiagohmm.com.br/backupPostgre/internal/config"\n)\n\n')
        for func_code in upload_funcs_code:
            func_code = func_code.replace('cfg Config', 'cfg config.Config')
            func_code = re.sub(r'^func (runSCP|parseSCPDest|uploadViaSFTP)', 
                               lambda m: f'func {m.group(1)[0].upper() + m.group(1)[1:]}', 
                               func_code, flags=re.MULTILINE)
            # fix recursive calls
            func_code = func_code.replace('uploadViaSFTP(', 'UploadViaSFTP(')
            func_code = func_code.replace('parseSCPDest(', 'ParseSCPDest(')
            f.write(func_code + '\n\n')

    main_code = main_code.replace('runSCP(', 'upload.RunSCP(')
    if '"github.thiagohmm.com.br/backupPostgre/internal/upload"' not in main_code:
        main_code = main_code.replace('import (', 'import (\n\t"github.thiagohmm.com.br/backupPostgre/internal/upload"\n')

    with open('main.go', 'w') as f:
        f.write(main_code)

if __name__ == '__main__':
    main()
