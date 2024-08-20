import subprocess
import sys

def main(instagram_handle, num_of_posts):
    # Use the full path to the Python interpreter
    python_path = r"C:\Users\hites\AppData\Local\Programs\Python\Python311\python.exe"
    
    # Run the first script to get the channel ID
    result = subprocess.run([python_path, 'instagramAPI_profile.py', instagram_handle], capture_output=True, text=True)
    
    # Prepare the command for the second script
    command = [python_path, 'instagramAPI_posts.py', instagram_handle, str(num_of_posts)]
    
    # Run the second script to fetch data and upload it to Google Drive
    subprocess.run(command)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 main_script.py <Instagram_handle> <num_of_posts>")
        sys.exit(1)
    
    instagram_handle = sys.argv[1]
    num_of_posts = int(sys.argv[2])
    
    main(instagram_handle, num_of_posts)
