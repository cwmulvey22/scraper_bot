import requests
import json
import time
import csv
import os
import sys
import subprocess

class InstagramPostDataFetcher:
    def __init__(self, api_key, instagram_handle, num_of_posts):
        self.api_key = api_key
        self.instagram_handle = instagram_handle
        self.num_of_posts = num_of_posts
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def trigger_data_fetch(self):
        url = f"{self.base_url}/trigger"
        data = json.dumps([{
            "url": f"https://www.instagram.com/{self.instagram_handle}/",
            "num_of_posts": self.num_of_posts
        }])
        params = {
            "dataset_id": "gd_lk5ns7kz21pck8jpis",
            "type": "discover_new",
            "discover_by": "profile_url",
            "format": "json"
        }
        return requests.post(url, headers=self.headers, data=data, params=params)

    def fetch_snapshot(self, snapshot_id, retries=20, delay=20):
        curl_command = f'curl -H "Authorization: Bearer {self.api_key}" "{self.base_url}/snapshot/{snapshot_id}?format=json"'
        for attempt in range(retries):
            result = subprocess.run(curl_command, shell=True, text=True, capture_output=True)
            print(f"Response from curl: {result.stdout}")

            try:
                response_data = json.loads(result.stdout)
                if isinstance(response_data, list) and response_data and 'shortcode' in response_data[0]:
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
    if len(sys.argv) < 3:
        print("Usage: python script.py <instagram_handle> <num_of_posts>")
        sys.exit(1)
    api_key = "7e4fe84a-14b3-4be5-b82c-4f2432600c58"
    instagram_handle = sys.argv[1]
    num_of_posts = int(sys.argv[2])
    fetcher = InstagramPostDataFetcher(api_key, instagram_handle, num_of_posts)

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
                json_file = f'output/instagram_post_{instagram_handle}/post_data.json'
                csv_file = f'output/instagram_post_{instagram_handle}/post_data.csv'
                fetcher.save_json(snapshot_data, json_file)
                fetcher.json_to_csv(snapshot_data, csv_file)
                print("Data processing completed successfully.")
            else:
                print("Failed to fetch snapshot data.")
        else:
            print("Snapshot ID not found in response.")
    else:
        print("Failed to trigger snapshot:", response.status_code, response.text)
