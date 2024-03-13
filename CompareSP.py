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

def get_stored_procedure_content(connection, database_name):
    cursor = connection.cursor()
    cursor.execute("USE " + database_name)
    cursor.execute("SELECT name, OBJECT_DEFINITION(OBJECT_ID(name)) as definition FROM sys.procedures")
    result = cursor.fetchall()
    stored_procedures = {row[0]: row[1] for row in result}
    return stored_procedures

def generate_drop_scripts(base_stored_procedures, target_stored_procedures):
    drop_scripts = []
    extra_stored_procedures = []
    for sp_name in target_stored_procedures:
        if sp_name not in base_stored_procedures:
            drop_scripts.append(f"DROP PROCEDURE {sp_name};")
            extra_stored_procedures.append(sp_name)
    return drop_scripts, extra_stored_procedures

base_stored_procedures_content = get_stored_procedure_content(conn_base, database_base)
target_stored_procedures_content = get_stored_procedure_content(conn_target, database_target)

differences_data = []

print("\nChecking for differences in stored procedure content:")
for sp_name in base_stored_procedures_content:
    if sp_name not in target_stored_procedures_content:
        print(f"\n - Stored procedure {sp_name} does not exist in the target database.")
        differences_data.append((sp_name, 'Stored Procedure', 'Missing in target'))
    elif base_stored_procedures_content[sp_name] != target_stored_procedures_content[sp_name]:
        print(f"\n - Stored procedure {sp_name} has differences in content.")
        differences_data.append((sp_name, 'Stored Procedure', 'Content different'))

# Generate drop scripts and extra stored procedures
drop_scripts, extra_stored_procedures = generate_drop_scripts(base_stored_procedures_content, target_stored_procedures_content)

# Write drop scripts to a file
with open('drop_sp_scripts.sql', 'w') as f:
    for script in drop_scripts:
        f.write(script + '\n')

print("\nDrop scripts generated successfully. Check 'drop_sp_scripts.sql'.")

if extra_stored_procedures:
    print("\nExtra stored procedures in", database_target + ":")
    for sp_name in extra_stored_procedures:
        print(" -", sp_name)

differences_df = pd.DataFrame(differences_data, columns=['Object Name', 'Object Type', 'Difference'])

differences_df.to_excel('differences.xlsx', index=False)

if update_target:
    cursor_target = conn_target.cursor()

    for sp_name in base_stored_procedures_content:
        if sp_name not in target_stored_procedures_content or base_stored_procedures_content[sp_name] != target_stored_procedures_content[sp_name]:
            sp_content_base = base_stored_procedures_content[sp_name]
            cursor_target.execute("IF EXISTS (SELECT * FROM sys.procedures WHERE name = ?) DROP PROCEDURE " + sp_name, sp_name)
            cursor_target.execute(sp_content_base)
            conn_target.commit()
            print(f"Stored procedure {sp_name} updated/created in the target database.")
            print("Generated SQL for creating/updating stored procedure:", sp_content_base)

    cursor_target.close()

conn_base.close()
conn_target.close()
