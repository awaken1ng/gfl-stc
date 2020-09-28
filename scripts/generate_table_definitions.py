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
    parser.add_argument('--shift', type=int, help='shift all further table IDs by 1 from specified table')
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
                if table_id == args.shift:
                    table_id += 1
                tables.append({'id': 5000 + table_id, 'name': table_name, 'fields': [], 'types': []})
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
                    field_type, field_name, = match.groups()

                    if field_type in ['sbyte', 'SByte']:
                        field_type = 'i8'
                    elif field_type in ['byte', 'Byte']:
                        field_type = 'u8'
                    elif field_type in ['short', 'Int16']:
                        field_type = 'i16'
                    elif field_type in ['ushort', 'UInt16']:
                        field_type = 'u16'
                    elif field_type in ['int', 'Int32']:
                        field_type = 'i32'
                    elif field_type in ['uint', 'UInt32']:
                        field_type = 'u32'
                    elif field_type in ['long', 'Int64']:
                        field_type = 'i64'
                    elif field_type in ['ulong', 'UInt64']:
                        field_type = 'u64'
                    elif field_type in ['float', 'Single']:
                        field_type = 'f32'
                    elif field_type in ['double', 'Double']:
                        field_type = 'f64'
                    elif field_type in ['string', 'String']:
                        field_type = 'string'
                    else:
                        raise Exception(f'unrecognized type: {field_type}')

                    table['fields'].append(field_name)
                    table['types'].append(field_type)

            fields = ','.join(table['fields'])
            types = ','.join(table['types'])
            out.writerow([table['id'], table['name'], fields, types])
