import pymarc
import pymysql
from dotenv import dotenv_values
from datetime import datetime

input_file_error = "input/UCLA MARC records/with Error/ucla20241210-marc.mrc"
input_file_no_error = "input/UCLA MARC records/without Error/ucla20240710-marc.mrc"
output_file = datetime.today().strftime('%Y%m%d') + "_corrected-ucla-records.mrc"


def get_eschol_db_connection(env):
    return pymysql.connect(
        host=env['ESCHOL_DB_SERVER_PROD'],
        user=env['ESCHOL_DB_USER_PROD'],
        password=env['ESCHOL_DB_PASSWORD_PROD'],
        database=env['ESCHOL_DB_DATABASE_PROD'],
        cursorclass=pymysql.cursors.DictCursor)


def get_eschol_sql_query(proquest_ucla_id):
    return f"""
        select
            i.id as eschol_id
        from
            items i,
            JSON_TABLE(
                attrs,
                "$.local_ids[*]"
                COLUMNS(local_id varchar(255) PATH "$.id",
                        local_type varchar(255) PATH "$.type")
            ) as json_t
        where
            json_t.local_type = 'other'
            and json_t.local_id like ('%ucla:{proquest_ucla_id}');"""


def main():
    env = dotenv_values(".env")
    reader = pymarc.MARCReader(open(input_file_error, 'rb'), force_utf8="True")
    mysql_conn = get_eschol_db_connection(env)
    corrected_records = []

    for record in reader:
        print("\nProcessing record:")
        print(record['035'])
        print(record['856'])

        # Split out the UCLA id and send the query
        proquest_ucla_id = record['035']['9'].split(')')[1]
        print(f"Querying eschol db for local_id ucla:{proquest_ucla_id}")
        with mysql_conn.cursor() as cursor:
            cursor.execute(get_eschol_sql_query(proquest_ucla_id))
            eschol_id = cursor.fetchone()['eschol_id']

        # Trim the shoulder and update the data
        record['856']['u'] = f'http://escholarship.org/uc/item/{eschol_id[2:]}'
        print(record['856'])
        corrected_records.append(record)

    # Save records to file
    with open(output_file, 'ab') as out:
        for record in corrected_records:
            out.write(record.as_marc())


# =========================
if __name__ == '__main__':
    main()
