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

def get_object_content(connection, database_name, object_type):
    cursor = connection.cursor()
    cursor.execute("USE " + database_name)
    cursor.execute(f"SELECT name, OBJECT_DEFINITION(OBJECT_ID(name)) as definition FROM sys.objects WHERE type_desc = '{object_type}'")
    result = cursor.fetchall()
    objects_content = {row[0]: row[1] for row in result}
    return objects_content

def generate_drop_scripts(base_objects, target_objects):
    drop_scripts = []
    extra_objects = []
    for obj_name in target_objects:
        if obj_name not in base_objects:
            drop_scripts.append(f"DROP FUNCTION {obj_name};")
            extra_objects.append(obj_name)
    return drop_scripts, extra_objects

base_functions_content = get_object_content(conn_base, database_base, 'SQL_SCALAR_FUNCTION')
target_functions_content = get_object_content(conn_target, database_target, 'SQL_SCALAR_FUNCTION')

differences_data = []
updated_functions = []

print("\nChecking for differences in function content:")
for func_name in base_functions_content:
    if func_name not in target_functions_content:
        print(f"\n - Function {func_name} does not exist in the target database.")
        differences_data.append((func_name, 'Function', 'Missing in target'))
    elif base_functions_content[func_name] != target_functions_content.get(func_name):
        print(f"\n - Function {func_name} has differences in content.")
        differences_data.append((func_name, 'Function', 'Content different'))
        updated_functions.append(func_name)

# Generate drop scripts for extra functions in the target database
drop_scripts, extra_functions = generate_drop_scripts(base_functions_content, target_functions_content)

# Write drop scripts to a file
with open('drop_function_scripts.sql', 'w') as f:
    for script in drop_scripts:
        f.write(script + '\n')

print("\nDrop scripts generated successfully. Check 'drop_function_scripts.sql'.")

# Print extra functions in the target database
if extra_functions:
    print("\nExtra functions in", database_target + ":")
    for func_name in extra_functions:
        print(" -", func_name)

differences_df = pd.DataFrame(differences_data, columns=['Object Name', 'Object Type', 'Difference'])
differences_df.to_excel('differences_functions.xlsx', index=False)

if update_target:
    cursor_target = conn_target.cursor()

    for func_name in base_functions_content:
        if func_name not in target_functions_content or base_functions_content[func_name] != target_functions_content.get(func_name):
            func_content_base = base_functions_content[func_name]
            cursor_target.execute("IF OBJECT_ID('" + func_name + "', 'FN') IS NOT NULL DROP FUNCTION " + func_name)
            cursor_target.execute(func_content_base)
            conn_target.commit()
            print(f"Function {func_name} updated/created in the target database.\n")
            print("Generated SQL for creating/updating function:\n", func_content_base)

    cursor_target.close()

conn_base.close()
conn_target.close()
