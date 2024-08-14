import subprocess
import sys

def main(youtube_handle, num_of_posts, from_date=None, until_date=None):
    # Run the first script to get the channel ID
    result = subprocess.run(['python', 'youtubeAPI_profile.py', youtube_handle], capture_output=True, text=True)
    
    # Extract the channel ID from the output
    channel_id = result.stdout.strip().split()[-1]  # Assuming the ID is at the end of the output
    print('result==',channel_id)
    # Prepare the command for the second script
    command = ['python', 'youtubeAPI_channel.py', channel_id, youtube_handle, str(num_of_posts)]
    if from_date:
        command.append(from_date)
    if until_date:
        command.append(until_date)
    
    # Run the second script to fetch data and upload it to Google Drive
    subprocess.run(command)

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print("Usage: python3 main_script.py <youtube_handle> <num_of_posts> [<from_date>] [<until_date>]")
        sys.exit(1)
    
    youtube_handle = sys.argv[1]
    num_of_posts = int(sys.argv[2])
    from_date = sys.argv[3] if len(sys.argv) > 3 else None
    until_date = sys.argv[4] if len(sys.argv) > 4 else None

    main(youtube_handle, num_of_posts, from_date, until_date)
