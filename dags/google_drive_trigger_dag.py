from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google_drive_sensor import GoogleDriveFileSensor, authenticate_google_account
import gzip
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from etl_pipeline import run_etl
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

FOLDER_ID = "1KM3o8HQNhV6ZO0LA_bkso-jp7Uv2QfsN"
LOCAL_DOWNLOAD = "/opt/Pipeline/downloads"

# ----------------------------------------------------
# DOWNLOAD FILE
# ----------------------------------------------------
def download_file(**kwargs):
    ti = kwargs["ti"]
    file_info = ti.xcom_pull(key="file_info")

    if not file_info:
        raise ValueError("No file_info XCom found from sensor")

    creds = authenticate_google_account()
    service = build("drive", "v3", credentials=creds)

    request = service.files().get_media(fileId=file_info["id"])

    os.makedirs(LOCAL_DOWNLOAD, exist_ok=True)
    file_path = f"{LOCAL_DOWNLOAD}/{file_info['name']}"

    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    return file_path


# ----------------------------------------------------
# COMPRESS FILE
# ----------------------------------------------------
def compress_file(**kwargs):
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="download_file")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError("Downloaded file missing")

    compressed_path = file_path + ".gz"

    with open(file_path, "rb") as f_in:
        with gzip.open(compressed_path, "wb") as f_out:
            f_out.write(f_in.read())

    return compressed_path


# ----------------------------------------------------
# UPLOAD COMPRESSED FILE BACK TO DRIVE
# ----------------------------------------------------
def upload_compressed_file(**kwargs):
    ti = kwargs["ti"]
    compressed_path = ti.xcom_pull(task_ids="compress_file")

    if not compressed_path or not os.path.exists(compressed_path):
        raise FileNotFoundError("Compressed file missing")

    creds = authenticate_google_account()
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": os.path.basename(compressed_path),
        "parents": [FOLDER_ID]
    }

    media = MediaIoBaseUpload(
        io.FileIO(compressed_path, "rb"),
        mimetype="application/gzip"
    )

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    return uploaded_file["id"]


# ----------------------------------------------------
# SEND EMAIL (HTML Format)
# ----------------------------------------------------
def send_email(**kwargs):
    ti = kwargs["ti"]
    file_info = ti.xcom_pull(key="file_info")
    compressed_path = ti.xcom_pull(task_ids="compress_file")

    # Compression calculations
    original_size = file_info.get("size", 0)
    compressed_size = os.path.getsize(compressed_path)
    compression_ratio = round(compressed_size / float(original_size), 3) if original_size else "N/A"

    # Constructing the email content (HTML format)
    html_content = f"""
    <html>
    <head>
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            table, th, td {{
                border: 1px solid black;
            }}
            th, td {{
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h2>Loan ETL – Processing Summary</h2>
        <p>The ETL pipeline has successfully processed 1 file(s) from Google Drive and stored the results.</p>
        
        <h3>File Compression Summary</h3>
        <table>
            <tr>
                <th>Filename</th>
                <th>Original Size</th>
                <th>Compressed Size</th>
                <th>Compression Ratio</th>
                <th>Google Drive Raw Object</th>
                <th>Google Drive Compressed Object</th>
            </tr>
            <tr>
                <td>{file_info['name']}</td>
                <td>{original_size} bytes</td>
                <td>{compressed_size} bytes</td>
                <td>{compression_ratio}</td>
                <td><a href="https://drive.google.com/file/d/{file_info['id']}">Raw File</a></td>
                <td><a href="https://drive.google.com/file/d/{file_info['id']}?export=download">Compressed File</a></td>
            </tr>
        </table>
    </body>
    </html>
    """

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = "bipinkumarthapa736@gmail.com"
    msg['To'] = "bipinkumarthapa736@gmail.com"
    msg['Subject'] = "Google Drive File Processed"

    # Attach the HTML content to the email
    msg.attach(MIMEText(html_content, 'html'))

    # Send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login("bipinkumarthapa736@gmail.com", "praogojxberjzdgv")
        server.send_message(msg)


# ----------------------------------------------------
# RUN ETL
# ----------------------------------------------------
def run_etl_task(**kwargs):
    file_path = kwargs["ti"].xcom_pull(task_ids="download_file")
    run_etl(file_path)


# ----------------------------------------------------
# DAG DEFINITION
# ----------------------------------------------------
with DAG(
    dag_id="google_drive_event_trigger",
    start_date=days_ago(1),
    schedule_interval="*/4 * * * *",
    catchup=False,
) as dag:

    wait_for_file = GoogleDriveFileSensor(
        task_id="wait_for_file",
        folder_id=FOLDER_ID,
        mode="reschedule",
        poke_interval=60
    )

    download = PythonOperator(
        task_id="download_file",
        python_callable=download_file
    )

    compress = PythonOperator(
        task_id="compress_file",
        python_callable=compress_file
    )

    upload = PythonOperator(
        task_id="upload_compressed_file",
        python_callable=upload_compressed_file
    )

    etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl_task
    )

    notify = PythonOperator(
        task_id="send_email",
        python_callable=send_email
    )

    wait_for_file >> download >> compress >> upload >> etl >> notify
