# Assuming the first script is named youtubeAPI_profile.py
import requests
import json
import time
import csv
import os
import sys

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

    def save_json(self, data, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to {filename}")

    def json_to_csv(self, json_data, output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(json_data.keys())
            csv_writer.writerow(json_data.values())

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
            print("Snapshot ID is: ", snapshot_id)
            snapshot_response = fetcher.fetch_snapshot(snapshot_id)
            if snapshot_response.status_code == 200:
                print("Snapshot data fetched successfully:")
                data = fetcher.parse_json_response(snapshot_response)
                json_file = f'output/youtube_{youtube_handle}/about.json'
                csv_file = f'output/youtube_{youtube_handle}/about.csv'
                fetcher.save_json(data, json_file)
                fetcher.json_to_csv(data, csv_file)
                
                # Print the id to pass to the second script
                if "id" in data:
                    print(data["id"])  # This is the channel_id to pass to the second script
                else:
                    print("ID not found in the data.")
            else:
                print("Failed to fetch snapshot data:", snapshot_response.status_code, snapshot_response.text)
        else:
            print("Snapshot ID not found in response.")
    else:
        print("Failed to trigger snapshot:", response.status_code, response.text)
