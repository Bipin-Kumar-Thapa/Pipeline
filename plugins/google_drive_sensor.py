from airflow.sensors.base import BaseSensorOperator
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle


def authenticate_google_account():
    creds = None
    token_path = '/opt/Pipeline/gdrive_credentials/token.pickle'
    credentials_path = '/opt/Pipeline/gdrive_credentials/credentials_oauth.json'

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            creds = flow.run_local_server(port=8080, open_browser=False)

        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return creds


class GoogleDriveFileSensor(BaseSensorOperator):

    def __init__(self, folder_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_id = folder_id

    def poke(self, context):
        creds = authenticate_google_account()
        service = build('drive', 'v3', credentials=creds)

        query = (
            f"'{self.folder_id}' in parents "
            f"and mimeType != 'application/vnd.google-apps.folder'"
        )

        results = service.files().list(
            q=query,
            fields="files(id, name, size, mimeType, trashed)"
        ).execute()

        files = results.get("files", [])

        # All CSV files
        csv_files = [
            f for f in files
            if f.get("mimeType") == "text/csv"
            and not f.get("trashed", False)
            and f["name"].lower().endswith(".csv")
        ]

        if not csv_files:
            self.log.info("No CSV files found. Waiting.")
            return False

        # All ZIP names in folder
        zip_names = {
            f["name"] for f in files
            if f["name"].lower().endswith(".zip")
            and not f.get("trashed", False)
        }

        # ✅ Find FIRST CSV that has NO zip
        for csv in csv_files:
            expected_zip = csv["name"] + ".zip"
            if expected_zip not in zip_names:
                self.log.info(f"Found unprocessed CSV: {csv['name']}")
                context["task_instance"].xcom_push(
                    key="file_info",
                    value=csv
                )
                return True

        # ❌ All CSVs already zipped
        self.log.info("All CSV files already have ZIPs. Waiting.")
        return False
