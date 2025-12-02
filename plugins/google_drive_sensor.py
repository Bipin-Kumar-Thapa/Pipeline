from airflow.sensors.base import BaseSensorOperator  # <-- Add this import
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle

# Standalone authenticate function
def authenticate_google_account():
    creds = None
    token_path = '/opt/Pipeline/gdrive_credentials/token.pickle'
    credentials_path = '/opt/Pipeline/gdrive_credentials/credentials_oauth.json'

    # Check if token.pickle exists to avoid re-authenticating every time
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # If there are no valid credentials, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            creds = flow.run_local_server(port=8080, open_browser=False)

        # Save the credentials for the next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return creds

class GoogleDriveFileSensor(BaseSensorOperator):
    def __init__(self, folder_id, filename=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_id = folder_id
        self.filename = filename  # Optional filter

    def poke(self, context):
        creds = authenticate_google_account()  # Now calling the function directly
        service = build('drive', 'v3', credentials=creds)

        query = f"'{self.folder_id}' in parents and mimeType!='application/vnd.google-apps.folder'"
        if self.filename:
            query += f" and name='{self.filename}'"

        results = service.files().list(
            q=query,
            fields="files(id, name, size, mimeType, modifiedTime)"
        ).execute()

        files = results.get('files', [])
        if not files:
            return False

        # Pick the newest file
        file_info = sorted(files, key=lambda x: x["modifiedTime"], reverse=True)[0]

        context["task_instance"].xcom_push(key="file_info", value=file_info)
        return True
