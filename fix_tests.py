import os

def fix_tests():
    with open('backup_test.go', 'r') as f:
        content = f.read()

    # Split config tests and main test
    config_tests = []
    main_tests = []
    
    current_test = []
    in_test = False
    test_name = ""
    for line in content.split('\n'):
        if line.startswith('func Test'):
            if current_test:
                if 'Validate' in test_name or 'Merge' in test_name:
                    config_tests.extend(current_test)
                else:
                    main_tests.extend(current_test)
            current_test = [line]
            test_name = line.split()[1].split('(')[0]
        else:
            if current_test:
                current_test.append(line)

    if current_test:
        if 'Validate' in test_name or 'Merge' in test_name:
            config_tests.extend(current_test)
        else:
            main_tests.extend(current_test)

    config_test_code = "package config\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n" + '\n'.join(config_tests)
    config_test_code = config_test_code.replace('Config{', 'Config{')
    config_test_code = config_test_code.replace('validateConfigSingleDB', 'ValidateConfigSingleDB')
    config_test_code = config_test_code.replace('validateConfigAllDBs', 'ValidateConfigAllDBs')
    config_test_code = config_test_code.replace('mergeWithEnvDefaults', 'MergeWithEnvDefaults')

    with open('internal/config/config_test.go', 'w') as f:
        f.write(config_test_code)

    main_test_code = "package main\n\nimport (\n\t\"bytes\"\n\t\"compress/gzip\"\n\t\"io\"\n\t\"os\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\n" + '\n'.join(main_tests)
    with open('backup_test.go', 'w') as f:
        f.write(main_test_code)

if __name__ == '__main__':
    fix_tests()
