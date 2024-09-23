import sys
import requests

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Presigned URL must be provided as a command-line argument.")
        exit(1)

    presigned_url = sys.argv[1]
    local_file_path = '/app/tld_files/org.txt'  # Adjust the path and filename as needed

    print(f"Downloading file to {local_file_path}")
    download_file(presigned_url, local_file_path)
    print("Download completed.")
