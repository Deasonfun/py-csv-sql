import os
import psycopg2

def create_table(conn, headers_str, file_name):
    cur = conn.cursor()

    cur.execute("drop table if exists %s" % file_name)

    create_table_string = "create table if not exists %s (" % file_name
    for header in headers:
        create_table_string = create_table_string + "%s VARCHAR(255)," % header

    create_table_string = create_table_string[:-1]
    create_table_string = create_table_string + ");"

    cur.execute(create_table_string)

    conn.commit()

def fill_table(conn, file_name, headers, rows):
   cur = conn.cursor()

   

   for row in rows:
        insert_into_table_string = "insert into {} (".format(file_name)

        for header in headers:
            insert_into_table_string = insert_into_table_string + "%s," % header

        insert_into_table_string = insert_into_table_string[:-1] + ") values ("

        row_elements = [s.strip() for s in row.split(",")]

        for e in row_elements:
            insert_into_table_string = insert_into_table_string + "'%s'," % e

        insert_into_table_string = insert_into_table_string[:-1] + ");"

        cur.execute(insert_into_table_string)

   conn.commit()

conn = psycopg2.connect(
    host="localhost",
    database="py_epa",
    user="postgres",
    password="password"
)



dataFiles = os.listdir('./data')
for file in dataFiles:
    if file.startswith("."):
        continue
    print(file)
    data = open("./data/%s" % file, "r")
    file_name = os.path.splitext(os.path.basename(file))
    headers = [s.strip() for s in data.readline().split(",")]
    rows = data.readlines()
    create_table(conn, headers, file_name[0])
    fill_table(conn, file_name[0], headers, rows)