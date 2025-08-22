import os
import requests

def download_file(url, local_path):
    try:
        print(f"Downloading {url} to {local_path}...")
        
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        print("Download complete.")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")
        return False
    except IOError as e:
        print(f"File I/O error: {e}")
        return False

def main():

    download_url = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"
    download_directory = "downloads"
    
    # Create the downloads directory if it doesn't exist
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
        print(f"Created directory: {download_directory}")
        
    file_name = download_url.split('/')[-1]
    local_file_path = os.path.join(download_directory, file_name)
    
    if download_file(download_url, local_file_path):
        print(f"File successfully saved at: {local_file_path}")
    else:
        print("File download failed.")

if __name__ == "__main__":
    main()