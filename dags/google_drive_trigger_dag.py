from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from google_drive_sensor import GoogleDriveFileSensor, authenticate_google_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

import smtplib
import os
import io
import zipfile

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from etl_pipeline import run_etl

# Timestamp imports
from datetime import datetime
import pytz


FOLDER_ID = "1KM3o8HQNhV6ZO0LA_bkso-jp7Uv2QfsN"
LOCAL_DOWNLOAD = "/opt/Pipeline/downloads"


# ----------------------------------------------------
# DOWNLOAD FILE
# ----------------------------------------------------
def download_file(**kwargs):
    ti = kwargs["ti"]
    file_info = ti.xcom_pull(key="file_info")

    creds = authenticate_google_account()
    service = build("drive", "v3", credentials=creds)

    meta = service.files().get(
        fileId=file_info["id"],
        fields="mimeType"
    ).execute()
    mime = meta["mimeType"]

    os.makedirs(LOCAL_DOWNLOAD, exist_ok=True)
    file_path = f"{LOCAL_DOWNLOAD}/{file_info['name']}"
    fh = io.FileIO(file_path, "wb")

    if mime == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_info["id"],
            mimeType="text/csv"
        )
    else:
        request = service.files().get_media(
            fileId=file_info["id"]
        )

    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    return file_path


# ----------------------------------------------------
# ETL WRAPPER
# ----------------------------------------------------
def run_etl_task(**kwargs):
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="download_file")

    insights, df = run_etl(file_path)

    df.to_csv(file_path, index=False)

    df_html = df.to_html(index=False, border=1)
    ti.xcom_push(key="etl_insights", value=insights)
    ti.xcom_push(key="etl_table_html", value=df_html)


# ----------------------------------------------------
# ZIP COMPRESSION
# ----------------------------------------------------
def compress_file(**kwargs):
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="download_file")
    zip_path = file_path + ".zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_path, arcname=os.path.basename(file_path))

    return zip_path


# ----------------------------------------------------
# UPLOAD COMPRESSED FILE
# ----------------------------------------------------
def upload_compressed_file(**kwargs):
    ti = kwargs["ti"]
    zip_path = ti.xcom_pull(task_ids="compress_file")

    creds = authenticate_google_account()
    service = build("drive", "v3", credentials=creds)

    media = MediaIoBaseUpload(
        io.FileIO(zip_path, "rb"),
        mimetype="application/zip"
    )

    uploaded = service.files().create(
        body={
            "name": os.path.basename(zip_path),
            "parents": [FOLDER_ID]
        },
        media_body=media,
        fields="id"
    ).execute()

    return uploaded["id"]


# ----------------------------------------------------
# SEND EMAIL SUMMARY
# ----------------------------------------------------
def send_email(**kwargs):
    ti = kwargs["ti"]

    nepal = pytz.timezone("Asia/Kathmandu")
    run_time = datetime.now(nepal).strftime("%Y-%m-%d %H:%M:%S (%Z)")

    file_info = ti.xcom_pull(key="file_info")
    zip_path = ti.xcom_pull(task_ids="compress_file")
    uploaded_id = ti.xcom_pull(task_ids="upload_compressed_file")

    insights = ti.xcom_pull(key="etl_insights", task_ids="run_etl")
    df_html = ti.xcom_pull(key="etl_table_html", task_ids="run_etl")

    original_size = int(file_info.get("size", 0))
    compressed_size = os.path.getsize(zip_path)

    size_decrease = original_size - compressed_size
    ratio = compressed_size / float(original_size)
    percent_decrease = round((1 - ratio) * 100, 1)

    raw_file_url = (
        f"https://drive.google.com/file/d/{file_info['id']}/view?usp=drivesdk"
    )
    compressed_file_url = (
        f"https://drive.google.com/file/d/{uploaded_id}/view?usp=drivesdk"
    )

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background:#eef2f3; padding:20px;">

    <div style="max-width:900px; margin:auto; background:white; padding:25px;
                border-radius:12px; box-shadow:0 6px 18px rgba(0,0,0,0.08);">

        <div style="background:#0c6b2f; padding:18px; border-radius:8px; color:white;">
            <h2 style="margin:0; font-size:22px;">
                Google Drive ETL – Processing Summary
            </h2>
        </div>

        <h3 style="margin-top:30px; color:#0c6b2f;">
            File Compression Summary
        </h3>

        <div style="border:1px solid #ccc; border-radius:6px;
                    padding:12px; margin-bottom:15px;">

            <div style="font-weight:bold; color:#0c6b2f;
                        font-size:16px; margin-bottom:8px;">
                {file_info['name']}.zip
            </div>

            <div><strong>Original Size:</strong> {original_size} bytes</div>
            <div><strong>Compressed Size:</strong> {compressed_size} bytes</div>
            <div><strong>Compression Ratio:</strong> {round(ratio, 3)}</div>
            <div><strong>File Size Decreased By:</strong> {size_decrease} bytes</div>
            <div><strong>Size Decreased By:</strong> {percent_decrease}%</div>

            <div style="margin-top:8px;">
                <strong>Raw File:</strong>
                <a href="{raw_file_url}">View Raw File</a>
            </div>

            <div>
                <strong>Compressed File:</strong>
                <a href="{compressed_file_url}">View Compressed</a>
            </div>

        </div>

        <p style="margin-top:40px; font-size:13px; color:#555;">
            Regards,<br>
            Bipin Kumar Thapa
        </p>

    </div>
    </body>
    </html>
    """

    msg = MIMEMultipart()
    msg["From"] = "bipinkumarthapa736@gmail.com"
    msg["To"] = "bipinbikramthapa@gmail.com"
    msg["Subject"] = "Google Drive File Processed"
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login("bipinkumarthapa736@gmail.com", "praogojxberjzdgv")
        s.send_message(msg)


# ----------------------------------------------------
# DAG
# ----------------------------------------------------
with DAG(
    dag_id="google_drive_event_trigger",
    start_date=days_ago(1),
    schedule_interval="*/4 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    wait = GoogleDriveFileSensor(
        task_id="wait_for_file",
        folder_id=FOLDER_ID,
        mode="reschedule",
        poke_interval=60,
    )

    download = PythonOperator(
        task_id="download_file",
        python_callable=download_file,
    )

    etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl_task,
    )

    compress = PythonOperator(
        task_id="compress_file",
        python_callable=compress_file,
    )

    upload = PythonOperator(
        task_id="upload_compressed_file",
        python_callable=upload_compressed_file,
    )

    notify = PythonOperator(
        task_id="send_email",
        python_callable=send_email,
    )

    wait >> download >> etl >> compress >> upload >> notify
