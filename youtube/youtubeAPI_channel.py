import os
import sys
import requests
import json
import time
import csv
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
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

    def get_or_create_folder(self, drive_service, folder_name, parent_folder_id=None):
        """Gets or creates a folder in Google Drive."""
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if items:
            return items[0]['id']
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]
            
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            return folder.get('id')

    def upload_csv_to_drive(self, csv_content, filename):
        SCOPES = ['https://www.googleapis.com/auth/drive.file']

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

        folder_name = f'youtube_data_{self.channel_id}'
        parent_folder_id = '1FUiSGG82YdbJjEjUOyZ2DbdyjifEfiHX'  # The shared folder ID
        folder_id = self.get_or_create_folder(drive_service, folder_name, parent_folder_id)

        query = f"name='{filename}' and '{folder_id}' in parents"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])

        if items:
            # File exists, add a new sheet
            spreadsheet_id = items[0]['id']
            sheet_title = f'{self.channel_id}_data_{int(time.time())}'

            # Add new sheet
            add_sheet_request = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_title,
                        }
                    }
                }]
            }

            sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=add_sheet_request).execute()

            # Write data to new sheet
            body = {
                'values': [row.split(',') for row in csv_content.strip().split('\n')]
            }
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_title}!A1',
                valueInputOption='RAW',
                body=body
            ).execute()

        else:
            # File does not exist, create a new spreadsheet
            spreadsheet = {
                'properties': {
                    'title': filename
                },
                'sheets': [{
                    'properties': {
                        'title': 'Sheet1'
                    }
                }]
            }

            spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')

            # Move the spreadsheet to the folder
            drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents='root',
                fields='id, parents'
            ).execute()

            # Write data to the first sheet
            body = {
                'values': [row.split(',') for row in csv_content.strip().split('\n')]
            }
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='Sheet1!A1',
                valueInputOption='RAW',
                body=body
            ).execute()

        # Set permissions for the file
        permissions = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': "hitesh@sute.app"
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permissions,
            fields='id'
        ).execute()

        print(f"Data uploaded to Google Sheets with ID: {spreadsheet_id}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python script.py <channel_id> <num_of_posts> <your_email>")
        sys.exit(1)
    api_key = "7e4fe84a-14b3-4be5-b82c-4f2432600c58"
    channel_id = sys.argv[1]
    youtube_handle=sys.argv[2]
    num_of_posts = int(sys.argv[3])
    # email = sys.argv[3]
    from_date = sys.argv[4] if len(sys.argv) > 4 else None
    until_date = sys.argv[5] if len(sys.argv) > 5 else None

    fetcher = YouTubeChannelDataFetcher(api_key, youtube_handle, num_of_posts, from_date, until_date)

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
                filename = f'youtube_channel_{youtube_handle}_data'
                fetcher.upload_csv_to_drive(csv_content, filename)
    else:
        print("Error triggering snapshot:", response.status_code, response.text)
