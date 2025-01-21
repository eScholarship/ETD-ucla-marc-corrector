import os
import io
import pymarc
import pymysql
from pprint import pprint
from dotenv import dotenv_values
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload


# =========================
def main():
    env = dotenv_values(".env")
    run_time = datetime.today().strftime('%Y%m%d')

    # Set up the Google Drive connection
    service = get_google_drive_service()

    # Get the input MARC files -- Will exit if there's no files to process.
    input_files = get_input_files(service, env['input_dir'])

    # Connect to MySQl
    mysql_conn = get_eschol_db_connection(env)

    # Create a backup subdir for corrected input files
    backup_subdir = create_backup_subdir(service, run_time, env['backup_dir'])

    # Empty array for corrected records, filename for corrected output.
    corrected_records = []
    output_file = f"{run_time}_corrected_ucla_records.mrc"

    # Loop the input files and process
    for input_file in input_files:
        print(f"Processing file: {input_file['name']}")
        file_content = get_file_content(service, input_file)
        reader = pymarc.MARCReader(file_content)

        # Each MARC file can have multiple records
        for record in reader:
            print(f"\nProcessing record:\n{record['035']}\n{record['856']}")

            # Split out the UCLA id and send the query
            proquest_ucla_id = record['035']['9'].split(')')[1]

            # Send the query w/ proquest ID
            print(f"Querying eschol db for local_id ucla:{proquest_ucla_id}")
            with mysql_conn.cursor() as cursor:
                cursor.execute(get_eschol_sql_query(proquest_ucla_id))
                eschol_id = cursor.fetchone()['eschol_id']

            # Trim the shoulder and update the data
            record['856']['u'] = f'http://escholarship.org/uc/item/{eschol_id[2:]}'
            print(f"Corrected 856:\n{record['856']}")
            corrected_records.append(record)

        print(f"Completed {input_file['name']}. Moving to backup subdir.")
        move_input_file_to_backup(service, input_file, env['input_dir'], backup_subdir)

    # Save correct MARC records to a file in the output dir
    save_corrected_file_to_output(service, run_time, corrected_records, env['output_dir'])

    print("Program complete. Exiting.")


# =========================
def get_google_drive_service():
    # If modifying these scopes, you need to create a new token.json.
    g_scopes = ["https://www.googleapis.com/auth/drive"]

    # Load access token
    if os.path.exists("token.json"):
        g_creds = Credentials.from_authorized_user_file("token.json", g_scopes)
    else:
        raise "No token.json found in this directory. Cannot connect to google drive."
        exit(1)

    # Create the Google Drive API service
    service = build("drive", "v3", credentials=g_creds)
    return service


def get_input_files(service, input_dir):
    parent_folder_query = f"'{input_dir}' in parents and name contains '.mrc'"

    results = (
        service.files().list(
            # pageSize=10,
            includeItemsFromAllDrives=True,
            includeTeamDriveItems=True,
            supportsAllDrives=True,
            supportsTeamDrives=True,
            q=parent_folder_query,
            fields="nextPageToken, files(id, name)",
        ).execute()
    )

    input_files = results.get("files", [])

    if not input_files:
        print("No input files found. Exiting.")
        exit(0)
    else:
        print("Input files found:")
        pprint(input_files)

    return input_files


def create_backup_subdir(service, run_time, parent_dir_id):
    backup_dir_metadata = {
        "name": f"{run_time}_input_files",
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_dir_id]}

    file = (
        service.files()
        .create(body=backup_dir_metadata, fields="id")
        .execute())

    print(f'Backup subdir ID: "{file.get("id")}".')
    return file.get("id")


def get_file_content(service, input_file):
    print(f"Downloading file content: {input_file['name']}")
    request = service.files().get_media(fileId=input_file['id'])
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False

    while done is False:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}.")

    return file.getvalue()


def move_input_file_to_backup(service, input_file, input_dir, backup_subdir):
    service.files().update(
        fileId=input_file['id'],
        addParents=backup_subdir,
        removeParents=input_dir
    ).execute()


def save_corrected_file_to_output(service, run_time, corrected_records, output_dir):

    # Pymarc doesn't handle bitstream output well,
    # creating a temp file and uploading is an easy workaround.
    temp_file = f"{run_time}_temp.mrc"
    with open(temp_file, 'ab') as out:
        for record in corrected_records:
            out.write(record.as_marc())

    file_metadata = {
        "name": f"{run_time}_corrected_records.mrc",
        "parents": [output_dir]}

    media = MediaFileUpload(
        temp_file,
        mimetype="application/marc",
        resumable=True)

    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute())

    print('\n\n----------------------------------------\n\n')
    print("\nCorrected file uploaded to Google Drive:")
    print(f"https://drive.google.com/file/d/{file.get('id')}")

    # Delete the temp.
    os.remove(temp_file)


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


# =========================
if __name__ == '__main__':
    main()
