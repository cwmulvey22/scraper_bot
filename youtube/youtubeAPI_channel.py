import requests
import json
import time
import csv
import os
import sys
import subprocess

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

    def fetch_snapshot(self, snapshot_id, retries=30, delay=60):
        curl_command = f'curl -H "Authorization: Bearer {self.api_key}" "{self.base_url}/snapshot/{snapshot_id}?format=json"'
        for attempt in range(retries):
            result = subprocess.run(curl_command, shell=True, text=True, capture_output=True)
            print(f"Response from curl: {result.stdout}")

            try:
                response_data = json.loads(result.stdout)
                if isinstance(response_data, list) and response_data and 'views' in response_data[0]:
                    print("Snapshot data is ready.")
                    return response_data
            except json.JSONDecodeError:
                print(f"Failed to decode JSON from curl response: {result.stderr}")

            if result.returncode == 0 and attempt < retries - 1:
                print(f"Try number {attempt + 1}. Data not ready yet, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Error executing curl: {result.stderr}")
                break

        return None


    def parse_json_response(self, response):
        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {str(e)}")
            return None

    def save_json(self, data, filename):
        """Saves JSON data to a file without brackets around the list."""
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as file:
            for entry in data:
                json.dump(entry, file)
                file.write('\n')

    def json_to_csv(self, json_data, output_file):
        """Converts a list of dictionaries to a CSV file."""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        if json_data:
            keys = json_data[0].keys()
            with open(output_file, 'w', newline='') as file:
                csv_writer = csv.DictWriter(file, fieldnames=keys)
                csv_writer.writeheader()
                csv_writer.writerows(json_data)

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
        snapshot_info = fetcher.parse_json_response(response.text)
        if snapshot_info and 'snapshot_id' in snapshot_info:
            snapshot_id = snapshot_info['snapshot_id']
            print("Snapshot ID is: ", snapshot_id)
            snapshot_data = fetcher.fetch_snapshot(snapshot_id)
            if snapshot_data:
                print("Snapshot data fetched successfully:")
                json_file = f'output/youtube_channel_{channel_id}/channel_data.json'
                csv_file = f'output/youtube_channel_{channel_id}/channel_data.csv'
                fetcher.save_json(snapshot_data, json_file)
                fetcher.json_to_csv(snapshot_data, csv_file)
                print("Data processing completed successfully.")
            else:
                print("Failed to fetch snapshot data.")
        else:
            print("Snapshot ID not found in response.")
    else:
        print("Failed to trigger snapshot:", response.status_code, response.text)