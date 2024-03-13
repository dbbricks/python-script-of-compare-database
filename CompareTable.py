import pyodbc
import pandas as pd
import sys

server = 'db.minvik.in'
username = 'rahul'
password = 'Rahul123'
database_base = 'QuizaleDev'
database_target = 'Quizale'

if len(sys.argv) > 1 and sys.argv[1].lower() == 'yes':
    update_target = True
else:
    update_target = False

conn_base = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database_base+';UID='+username+';PWD='+password)
conn_target = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database_target+';UID='+username+';PWD='+password)

def get_table_columns(connection, database_name):
    cursor = connection.cursor()
    cursor.execute("USE " + database_name)
    cursor.execute("""
        SELECT 
            TABLE_NAME, 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH, 
            IS_NULLABLE, 
            COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS
    """)
    result = cursor.fetchall()
    table_columns = {}
    for row in result:
        table_name = row.TABLE_NAME
        column_name = row.COLUMN_NAME
        data_type = row.DATA_TYPE
        size = row.CHARACTER_MAXIMUM_LENGTH if row.CHARACTER_MAXIMUM_LENGTH is not None else ''
        is_nullable = 'NULL' if row.IS_NULLABLE == 'YES' else 'NOT NULL'
        is_identity = row.IS_IDENTITY
        if table_name in table_columns:
            table_columns[table_name].append((column_name, data_type, size, is_nullable, is_identity))
        else:
            table_columns[table_name] = [(column_name, data_type, size, is_nullable, is_identity)]
    return table_columns


def get_table_constraints(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"EXEC sp_helpconstraint '{table_name}'")
    result = cursor.fetchall()
    constraints = {}
    for row in result:
        constraint_name = row[0]
        if constraint_name not in constraints:
            constraints[constraint_name] = {'type': '', 'columns': []}
        if len(row) > 1:
            constraint_type = row[1]
            constraints[constraint_name]['type'] = constraint_type
        if len(row) > 2:
            constraints[constraint_name]['columns'].append(row[2])
    return constraints

def sync_table_data(source_conn, target_conn, table_name):
    cursor_source = source_conn.cursor()
    cursor_target = target_conn.cursor()

    try:
        # Check if the table has an identity column before enabling IDENTITY_INSERT
        cursor_target.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1")
        identity_column = cursor_target.fetchone()
        if identity_column:
            cursor_target.execute(f"SET IDENTITY_INSERT {table_name} ON")  # Enable IDENTITY_INSERT

        cursor_target.execute(f"TRUNCATE TABLE {table_name}")
    except pyodbc.Error as e:
        error_code = e.args[0]
        if error_code == '42000':
            print(f"Warning: The table '{table_name}' cannot be truncated due to foreign key constraints.")
        else:
            raise

    cursor_source.execute(f"SELECT * FROM {table_name}")
    data = cursor_source.fetchall()

    columns = ', '.join([f'[{column[0]}]' for column in base_tables_columns[table_name]])
    placeholders = ', '.join(['?' for _ in base_tables_columns[table_name]])
    insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor_target.executemany(insert_statement, data)
    target_conn.commit()

    if identity_column:
        cursor_target.execute(f"SET IDENTITY_INSERT {table_name} OFF")  # Disable IDENTITY_INSERT

    cursor_source.close()
    cursor_target.close()

base_tables_columns = get_table_columns(conn_base, database_base)
target_tables_columns = get_table_columns(conn_target, database_target)

differences_data = []

print("Missing tables in", database_target, "compared to", database_base + ":")
for table_base in base_tables_columns.keys():
    if table_base not in target_tables_columns:
        print(" -", table_base)
        differences_data.append((table_base, 'Missing table'))

print("\nMissing columns in tables in", database_target, "compared to", database_base + ":")
for table_base, columns_base in base_tables_columns.items():
    if table_base in target_tables_columns:
        columns_target = target_tables_columns[table_base]
        for column_base, data_type_base, size_base, is_nullable_base, is_identity_base in columns_base:
            found = False
            for column_target, _, _, _, _ in columns_target:
                if column_base == column_target:
                    found = True
                    break
            if not found:
                print(" - Column", column_base, "in table", table_base)
                differences_data.append((table_base, f"Missing column: {column_base}"))

# Check for tables in target database that are not in base database
for table_target in target_tables_columns.keys():
    if table_target not in base_tables_columns:
        print("\n\n -", table_target, "(Extra table in", database_target + ")")
        differences_data.append((table_target, 'Extra table'))

# Check for columns in tables in target database that are not in base database
for table_target, columns_target in target_tables_columns.items():
    if table_target in base_tables_columns:
        columns_base = base_tables_columns[table_target]
        for column_target, _, _, _, _ in columns_target:
            found = False
            for column_base, _, _, _, _ in columns_base:
                if column_target == column_base:
                    found = True
                    break
            if not found:
                print("\n\n - Column", column_target, "in table", table_target, "(Extra column in", database_target + ")")
                differences_data.append((table_target, f"Extra column: {column_target}"))

# Generate SQL scripts for dropping extra tables and columns
drop_scripts = []
for table_target in target_tables_columns.keys():
    if table_target not in base_tables_columns:
        drop_scripts.append(f"DROP TABLE {table_target};")
    else:
        columns_target = target_tables_columns[table_target]
        columns_base = base_tables_columns[table_target]
        for column_target, _, _, _, _ in columns_target:
            found = False
            for column_base, _, _, _, _ in columns_base:
                if column_target == column_base:
                    found = True
                    break
            if not found:
                drop_scripts.append(f"ALTER TABLE {table_target} DROP COLUMN {column_target};")

with open('drop_scripts.sql', 'w') as f:
    for script in drop_scripts:
        f.write(script + '\n')

print("\nDrop scripts generated successfully. Check 'drop_scripts.sql'.")

differences_df = pd.DataFrame(differences_data, columns=['Table', 'Difference'])

differences_df.to_excel('differences.xlsx', index=False)

if update_target:
    cursor_target = conn_target.cursor()

    def get_primary_key(connection, table_name):
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{table_name}'")
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    # Inside the loop where tables are processed:
    for table_base in base_tables_columns.keys():
        if table_base not in target_tables_columns:
            primary_key = get_primary_key(conn_base, table_base)
            constraints = get_table_constraints(conn_base, table_base)
            if primary_key:
                primary_key_constraint = f"CONSTRAINT [PK_{table_base}] PRIMARY KEY CLUSTERED ([{primary_key}] ASC)"
            else:
                primary_key_constraint = ""
            create_script = f"CREATE TABLE {table_base} (\n"
            column_definitions = []

            # for manage data types and sizes
            for column, data_type, size, is_nullable, is_identity in base_tables_columns[table_base]:
                column_def = f"\t[{column}] {data_type}"
                if data_type in ['varchar', 'nvarchar'] and size == -1:
                    column_def += '(max)'
                elif data_type in ['varchar', 'nvarchar']:
                    column_def += f'({size})'
                elif data_type == 'numeric':
                    column_def += f'({size})'
                column_def += f' {is_nullable}'
                column_definitions.append(column_def)

            # Add constraints
            for constraint_name, constraint_info in constraints.items():
                if constraint_info['type'] == 'FOREIGN KEY':
                    column_list = ', '.join([f"[{col}]" for col in constraint_info['columns']])
                    create_script += f",\n\tCONSTRAINT [{constraint_name}] FOREIGN KEY ({column_list}) REFERENCES {constraint_info['reference_table']} ({constraint_info['reference_column']})"
                elif constraint_info['type'] == 'CHECK':
                    check_def = constraint_info['check_def']
                    create_script += f",\n\tCONSTRAINT [{constraint_name}] CHECK {check_def}"
                elif constraint_info['type'] == 'UNIQUE':
                    column_list = ', '.join([f"[{col}]" for col in constraint_info['columns']])
                    create_script += f",\n\tCONSTRAINT [{constraint_name}] UNIQUE ({column_list})"
                elif constraint_info['type'] == 'DEFAULT':
                    default_value = constraint_info['default_value']
                    create_script += f",\n\tCONSTRAINT [{constraint_name}] DEFAULT {default_value}"

            create_script += ',\n'.join(column_definitions)
            create_script += f",\n {primary_key_constraint}\n);\n" if primary_key_constraint else "\n);\n"
            print("\nGenerated SQL for creating table:", create_script)
            cursor_target.execute(create_script)
            conn_target.commit()

    for table_base, columns_base in base_tables_columns.items():
        if table_base in target_tables_columns:
            columns_target = target_tables_columns[table_base]
            for column_base, data_type_base, size_base, is_nullable_base, is_identity_base in columns_base:
                found = False
                for column_target, _, _, _, _ in columns_target:
                    if column_base == column_target:
                        found = True
                        break
                if not found:
                    alter_script = f"ALTER TABLE {table_base} ADD [{column_base}] {data_type_base}"
                    if data_type_base in ['varchar', 'nvarchar'] and size_base == -1:
                        alter_script += '(max)'
                    elif data_type_base in ['varchar', 'nvarchar']:
                        alter_script += f'({size_base})'
                    alter_script += f' {is_nullable_base}'
                    cursor_target.execute(alter_script)
                    conn_target.commit()

    # for sync data
    table_to_sync = input("Enter the name of the table to sync data: ")
    sync_table_data(conn_base, conn_target, table_to_sync)
    print("Data for table", table_to_sync, "synced successfully.")

    cursor_target.close()

conn_base.close()
conn_target.close()
