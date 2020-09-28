#!/usr/bin/env python
import re
import argparse
import csv


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('headers')
    parser.add_argument('output')
    parser.add_argument('--region', action='store', default='UNKNOWN_REGION')
    parser.add_argument('--version', action='store', default='UNKNOWN_VERSION')
    args = parser.parse_args()

    with open(args.headers) as headers, open(args.output, 'w') as out:
        out = csv.writer(out, delimiter=';')
        out.writerow([args.region, args.version])

        # find table names from CmdDef enum
        # read until we bump into the enum
        while line := headers.readline():
            if line.find('enum CmdDef') != -1:
                break
        assert headers.readline().strip() == '{'

        # read enum fields until we bump into closing bracket
        tables = []
        table_id = 0
        while line := headers.readline().strip():
            if line == '}':
                break

            if match := re.search(r'CmdDef stc(.+)List', line):
                table_name, = match.groups()
                tables.append({'id': 5000 + table_id, 'name': table_name, 'fields': []})
                table_id += 1

        # find field names
        for table in tables:
            table_class = f"class Stc{table['name']}"
            # read until we bump into table class
            while line := headers.readline():
                if line.find(table_class) != -1:
                    break
                if len(line) == 0:  # start from the top if we reached EOF
                    headers.seek(0)
            assert headers.readline().strip() == '{'
            assert headers.readline().strip() == '// Fields'

            while line := headers.readline().strip():
                if line in ['// Methods', '}']:
                    break
                if line.find('DelegateBridge') != -1:
                    # skip lua hot patching related lines
                    continue
                if match := re.search(r'.+ (.+) (.+);', line):
                    _field_type, field_name, = match.groups()
                    table['fields'].append(field_name)

            fields = ','.join(table['fields'])
            out.writerow([table['id'], table['name'], fields])
