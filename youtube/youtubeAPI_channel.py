import os
import requests
import json
import time
import csv
import sys
import io
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from dotenv import load_dotenv

load_dotenv()

class YouTubeChannelDataFetcher:
    def __init__(self, api_key, channel_id, num_of_posts, from_date, until_date):
        self.api_key = api_key
        self.channel_id = channel_id
        self.num_of_posts = num_of_posts
        self.from_date = from_date
        self.until_date = until_date
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.channel_url = f"https://www.youtube.com/channel/{self.channel_id}"

    def trigger_data_fetch(self):
        url = f"{self.base_url}/trigger"
        data = json.dumps([{
            "url": self.channel_url,
            "num_of_posts": self.num_of_posts,
            "start_date": self.from_date,
            "end_date": self.until_date
        }])
        params = {
            "dataset_id": "gd_lk56epmy2i5g7lzu0k",
            "type": "discover_new",
            "discover_by": "url",
            "format": "json",
            "uncompressed_webhook": "true"
        }
        return requests.post(url, headers=self.headers, data=data, params=params)

    def fetch_snapshot(self, snapshot_id, retries=20, delay=60):
        print(snapshot_id)
        url = f"{self.base_url}/snapshot/{snapshot_id}?format=json"
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                response_data = response.json()
                if isinstance(response_data, list) and response_data and 'views' in response_data[0]:
                    print("Snapshot data is ready.")
                    return response_data
                else:
                    print(f"Data not ready yet, retrying in {delay} seconds...")
                    time.sleep(delay)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching snapshot data: {str(e)}")
                break

        return None

    def save_json(self, data, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as file:
            for entry in data:
                json.dump(entry, file)
                file.write('\n')

    def json_to_csv(self, json_data):
        if json_data:
            keys = json_data[0].keys()
            output = io.StringIO()
            csv_writer = csv.DictWriter(output, fieldnames=keys)
            csv_writer.writeheader()
            csv_writer.writerows(json_data)
            return output.getvalue()
        return ""

    def upload_csv_to_drive(self, csv_content, filename, shared_folder_id="1FUiSGG82YdbJjEjUOyZ2DbdyjifEfiHX"):
        SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets']
        credentials_dict = {
            "type": os.getenv("GOOGLE_TYPE"),
            "project_id": os.getenv("GOOGLE_PROJECT_ID"),
            "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            # "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
            "private_key": """-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCY1AAG/kuKQYf1\nP+c44ixOWTU4M4+1o+Lg6XJUeEA8Om2cme6EwosuA+sXDLG5pPcZ0nJWlPJXM+Yu\nRfnjpj2mlSZPr88C/NU62yBE3HXPo3iEMHj8LaBhsztVuCwgfGEXBxvNTJz6tYcr\nH2CUAr0hbuECUKgGSLVwakVHs7C4HKaSe3qbm9jFC1QgO+DGIZYiz/axyFt0SG3x\nHgt5J6Xkx+Qg0v5lpn2wydVb8+w4nl04+0dWXFesGx5U6nlNKUUjHm571u8s415o\nagn81oDKNvm1LGMTfED5sbMCwf8jnT5rjwGSUJbt+jFMIM2yWv4NY955oM8Ckj0v\nLnSbmDV3AgMBAAECggEAC6M1H1M08GG+FyRXGKNMaWjsnwt6GYRRzfuo25rOlWeD\nmkW0foKL86STvc0XKYJQ2LQIReSQsag/km18smOh1TINWUsTcfBL0nAGPIgnJnkV\nv8crJ1TYHih2T7g3EAA4qna9nLwFRigVk2iPw6WTvlyBoNoAeX4G5vkIroWV4ucZ\nebdojuegduP0ZMFW48TLxMt4GWZAzTovdspk3Y8wU20oN0tKTa3r0A6t5q5FDxac\nnCjwxxp6a9Z2qEAISb6NSYAAfINkbGVUfNiv2VA9FI0u54xYUsnNSAzrCyrKhy1y\nTmfQHGstdoqz0fk6FBXAYK2CTLWx45RRuaZr6IlhKQKBgQDMuRewuzhkU5rLmPh7\nkUy4K0HqT2B+QszJOtme3eXUTeiWJbd/DdA/M6gVAUKiIfISj+W9a/F/7dxBDFkn\nIvCaN+7JFrlPWY9HfZXfcjjKF3GlVTNNvs+jYek67rAFzzicM+uhWPnk+VgZDQ8O\nXxF8HOYiRNT2bSJOo2T6oERO7wKBgQC/G2PTZtr1gbR85hMd8tp7chERSkCPUyKB\nFk2s6QVyjgzMVDyHKuOH0dMJnT4C/2ANrfRt7kBkkGrNCW+4GrzcDxrsL0kGdPq8\nPW8z5zqp6Q5ATkM2+Z1m/owc3fxKbrxT16eORvgOsK5jNWW3+UthwP7aIIi33hbj\nsr1rKK+B+QKBgQCPjNIByKW4I6+NR8wkyTOkiCCGLfaZUjnKeIuUDEBV5/NJJVVP\nr93wE0auw913VpopTeFoO0Jx09X3frMc8DEJ0mKLenWiIEiJdpQaxDrx6hJ0PhPl\nVgC+ra8e9bNTv4QQc4+r6XoAhp6xoiiGiT73akQsj1tNGCVQQt5RpwN+3QKBgDNx\nwhukcojoU5fTr42+VEYq3KFU5bAvZvhs8pf7WnYN+y/99RVF3F4xg6fw5kKUUF0e\nWNBG9JqdrcJoKeTbfb+XaV3vFK9iSiTmPMsyEb6veCCjcMCZzV9uYnVa5JF84cGI\nKhjIzfnWYfte4nT17O7xryk03NjyNiMxeIAiQayxAoGADogpGXDmPy+XO41jRDG9\nDXgAmPCyNbdNn6GdcaWm6D1wYjwQSExqB2UGJGM4U1WRktMyU4XGVAwN/fnnEgux\n1MEqeFA4W76PQ01ECigLnjsJIKq83FGuaylNWvUc9Z3njSyZ3JoKoveCMPrIWezN\nrsm/s2njbqOT4m0RArQIKzU=\n-----END PRIVATE KEY-----\n""",
            "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
            "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL"),
        }

        credentials = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=credentials)
        sheets_service = build('sheets', 'v4', credentials=credentials)

        # Check if the folder already exists
        folder_name = filename.split('.')[0]
        query = f"'{shared_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}'"
        folders = drive_service.files().list(q=query, fields='files(id, name)').execute().get('files', [])
        
        if folders:
            folder_id = folders[0]['id']
        else:
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [shared_folder_id]  
            }
            folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')

        # Check if the CSV file exists in the folder
        query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.spreadsheet' and name = '{filename}'"
        files = drive_service.files().list(q=query, fields='files(id, name)').execute().get('files', [])

        if files:
            spreadsheet_id = files[0]['id']

            # Get the list of existing sheets
            spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            existing_sheets = [sheet['properties']['title'] for sheet in spreadsheet.get('sheets', [])]

            # Generate a unique sheet name
            base_sheet_name = f"{folder_name}_data"
            sheet_name = base_sheet_name
            i = 1
            while sheet_name in existing_sheets:
                sheet_name = f"{base_sheet_name}_{i}"
                i += 1

            # Create the new sheet
            add_sheet_request = {
                'requests': [
                    {
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }
                ]
            }
            sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=add_sheet_request).execute()

            # Convert CSV content to a list of rows
            csv_rows = list(csv.reader(io.StringIO(csv_content)))

            # Prepare data to be inserted
            data = {
                'range': f"{sheet_name}!A1",
                'majorDimension': 'ROWS',
                'values': csv_rows
            }

            # Insert data into the newly created sheet
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body=data
            ).execute()
        else:
            # CSV doesn't exist, create a new file and upload
            file_metadata = {
                'name': filename,
                'parents': [folder_id],
                'mimeType': 'application/vnd.google-apps.spreadsheet'
            }
            media = MediaIoBaseUpload(io.BytesIO(csv_content.encode('utf-8')), mimetype='text/csv')
            file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            spreadsheet_id = file.get('id')

            print(f"File uploaded to Google Drive in folder with ID: {folder_id}, File ID: {spreadsheet_id}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <channel_id> <num_of_posts> <from_day> <from_month> <from_year> <until_day> <until_month> <until_year>")
        sys.exit(1)
    api_key = "7e4fe84a-14b3-4be5-b82c-4f2432600c58"
    channel_id = sys.argv[1]
    num_of_posts = int(sys.argv[2])
    from_date = ""
    until_date = ""
    fetcher = YouTubeChannelDataFetcher(api_key, channel_id, num_of_posts, from_date, until_date)

    response = fetcher.trigger_data_fetch()
    if response.status_code == 200:
        print("Snapshot trigger successful. Fetching data...")
        snapshot_info = response.json()
        if snapshot_info and 'snapshot_id' in snapshot_info:
            snapshot_id = snapshot_info['snapshot_id']
            print("Snapshot ID is: ", snapshot_id)
            snapshot_data = fetcher.fetch_snapshot(snapshot_id)
            if snapshot_data:
                print("Snapshot data fetched successfully:")
                csv_content = fetcher.json_to_csv(snapshot_data)
                fetcher.upload_csv_to_drive(csv_content, f'youtube_channel_{channel_id}_data.csv')
                print("Data processing and upload completed successfully.")
            else:
                print("Failed to fetch snapshot data.")
        else:
            print("Snapshot ID not found in response.")
    else:
        print("Failed to trigger snapshot:", response.status_code, response.text)
