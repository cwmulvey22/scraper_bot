import requests
import json
import time
import csv
import io
import os
import sys
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

class YouTubeProfileDataFetcher:
    def __init__(self, api_key, youtube_handle):
        self.api_key = api_key
        self.youtube_handle = youtube_handle
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.youtube_url = f"https://www.youtube.com/@{youtube_handle}/about"

    def trigger_data_fetch(self):
        url = f"{self.base_url}/trigger"
        data = json.dumps([{"url": self.youtube_url}])
        params = {
            "dataset_id": "gd_lk538t2k2p1k3oos71",
            "format": "json",
            "uncompressed_webhook": "true"
        }
        return requests.post(url, headers=self.headers, data=data, params=params)

    def fetch_snapshot(self, snapshot_id, retries=20, delay=10):
        url = f"{self.base_url}/snapshot/{snapshot_id}"
        for attempt in range(retries):
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200 and response.json().get('status') != 'running':
                return response
            else:
                print(f"Try number {attempt + 1}. Snapshot is not ready yet, retrying in {delay} seconds...")
                time.sleep(delay)
        return response

    def parse_json_response(self, response):
        try:
            return response.json()
        except json.JSONDecodeError:
            start = response.text.index('{')
            end = response.text.rindex('}') + 1
            return json.loads(response.text[start:end])

    def json_to_csv(self, json_data):
        if json_data:
            def clean_field(value):
                if isinstance(value, str):
                    return value.replace('\n', ' ').replace(',', ';')
                elif isinstance(value, list):
                    
                    return ';'.join(clean_field(str(item)) for item in value)
                elif isinstance(value, dict):
                    
                    return json.dumps(value).replace(',', ';').replace('\n', ' ')
                return value

            
            cleaned_data = {k: clean_field(v) for k, v in json_data.items()}

            output = io.StringIO()
            csv_writer = csv.DictWriter(output, fieldnames=cleaned_data.keys(), quoting=csv.QUOTE_ALL)
            csv_writer.writeheader()
            csv_writer.writerow(cleaned_data)
            return output.getvalue()
        return ""

    def get_or_create_folder(self, drive_service, folder_name, parent_folder_id=None):
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

        folder_name = f'youtube_data_{self.youtube_handle}'
        parent_folder_id = '1FUiSGG82YdbJjEjUOyZ2DbdyjifEfiHX'
        folder_id = self.get_or_create_folder(drive_service, folder_name, parent_folder_id)

        query = f"name='{filename}' and '{folder_id}' in parents"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])

        if items:
            spreadsheet_id = items[0]['id']
            sheet_title = f'{self.youtube_handle}_profile_data_{int(time.time())}'

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

            drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents='root',
                fields='id, parents'
            ).execute()

            body = {
                'values': [row.split(',') for row in csv_content.strip().split('\n')]
            }
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='Sheet1!A1',
                valueInputOption='RAW',
                body=body
            ).execute()

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

        print(f"Profile data uploaded to Google Sheets with ID: {spreadsheet_id}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python youtubeAPI_profile.py <youtube_handle>")
        sys.exit(1)
    api_key = "7e4fe84a-14b3-4be5-b82c-4f2432600c58"
    youtube_handle = sys.argv[1]
    fetcher = YouTubeProfileDataFetcher(api_key, youtube_handle)
    
    response = fetcher.trigger_data_fetch()
    if response.status_code == 200:
        print("Snapshot trigger successful. Fetching data...")
        snapshot_info = fetcher.parse_json_response(response)
        if snapshot_info and 'snapshot_id' in snapshot_info:
            snapshot_id = snapshot_info['snapshot_id']
            print("Snapshot ID is from profile: ", snapshot_id)
            snapshot_response = fetcher.fetch_snapshot(snapshot_id)
            if snapshot_response.status_code == 200:
                print("Snapshot data fetched successfully:")
                data = fetcher.parse_json_response(snapshot_response)
                csv_content = fetcher.json_to_csv(data)
                filename = f'youtube_channel_{youtube_handle}_data'
                fetcher.upload_csv_to_drive(csv_content, filename)
                
                if "id" in data:
                    print(data["id"])
                else:
                    print("ID not found in the data.")
            else:
                print("Failed to fetch snapshot data:", snapshot_response.status_code, snapshot_response.text)
        else:
            print("Snapshot ID not found in response.")
    else:
        print("Failed to trigger snapshot:", response.status_code, response.text)
