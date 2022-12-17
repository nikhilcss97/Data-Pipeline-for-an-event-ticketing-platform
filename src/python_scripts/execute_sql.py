import sys

import snowflake.connector as sf


def execute_ingestion_sql_stmt(filepath, account, database, password, schema, username, warehouse):
    account_name = account
    username = username
    password = password
    warehouse_name = warehouse
    database_name = database
    schema_name = schema
    print("RUN DETAILS",
          account_name + "," + username + "," + password + "," + warehouse_name + "," + database_name + ","
          + schema_name)

    try:
        sf_connection = sf.connect(
            user=username,
            password=password,
            account=account_name,
            warehouse=warehouse_name,
            database=database_name,
            schema=schema_name
        )
        sfq = sf_connection.cursor()
        print("EXECUTING FILE: ", filepath)
        sql_file = filepath
        sql_file_r = open(sql_file)
        sql_file_s = sql_file_r.read()

        sql_strings = sql_file_s.split(';')

        print("SQL STRINGS: ", sql_strings)
        for sql in sql_strings:
            sql = sql.strip()
            if sql != '\n' or sql != "":
                print("Running SQL STATEMENT- " + sql)
                sfq.execute(sql)

        res_tuple = sfq.fetchall()[0]
        count = res_tuple[0]
        sfq.close()
        sf_connection.close()

        print('END OF SQL SCRIPTS')
        print("DataType of count variable", type(count))
        return count
    except Exception as e:
        print(e)
        print('Connection failed. Check credentials')


def main():
    file_path = sys.argv[1]
    account = sys.argv[2]
    database = sys.argv[3]
    password = sys.argv[4]
    schema = sys.argv[5]
    username = sys.argv[6]
    warehouse = sys.argv[7]
    execute_ingestion_sql_stmt(file_path, account, database, password, schema, username, warehouse)


if __name__ == "__main__":
    main()
