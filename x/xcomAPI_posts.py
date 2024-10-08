import requests
import json
import time
import csv
import os
import sys

class XComPostDataFetcher:
    def __init__(self, api_key, post_id):
        self.api_key = api_key
        self.post_id = post_id
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.post_url = f"https://x.com/FabrizioRomano/status/{self.post_id}"

    def trigger_data_fetch(self):
        url = f"{self.base_url}/trigger"
        data = json.dumps([{"url": self.post_url}])
        params = {
            "dataset_id": "gd_lwxkxvnf1cynvib9co",
            "format": "json",
            "uncompressed_webhook": "true"
        }
        return requests.post(url, headers=self.headers, data=data, params=params)

    def fetch_snapshot(self, snapshot_id, retries=30, delay=60):
        url = f"{self.base_url}/snapshot/{snapshot_id}"
        for attempt in range(retries):
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200 and response.json().get('status') != 'running':
                return response
            else:
                print(f"Try number {attempt+1}. Snapshot is not ready yet, retrying in {delay} seconds...")
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
        print("Usage: python script.py <post_id>")
        sys.exit(1)
    api_key = "7e4fe84a-14b3-4be5-b82c-4f2432600c58"
    post_id = sys.argv[1]
    fetcher = XComPostDataFetcher(api_key, post_id)
    
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
                json_file = f'output/xcom_posts/{post_id}.json'
                csv_file = f'output/xcom_posts/{post_id}.csv'
                fetcher.save_json(data, json_file)
                fetcher.json_to_csv(data, csv_file)
            else:
                print("Failed to fetch snapshot data:", snapshot_response.status_code, snapshot_response.text)
        else:
            print("Snapshot ID not found in response.")
    else:
        print("Failed to trigger snapshot:", response.status_code, response.text)
