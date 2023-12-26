import os
import subprocess
import re
import concurrent.futures

def extract_sequence_number(filename):
    match = re.search(r'part-(\d+)-of-\d+\.csv', filename)
    return int(match.group(1)) if match else None

def get_last_saved_file_number(destination_bucket, dir_name):
    cmd = f'gsutil ls {destination_bucket}/{dir_name}/ | grep ".csv$"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    files = result.stdout.splitlines()
    if not files:
        return 0
    last_file = sorted(files, key=lambda f: extract_sequence_number(f))[-1]
    return extract_sequence_number(last_file)

def process_file(gz_file_url, local_storage_path, destination_bucket, dir_name):
    gz_file = os.path.basename(gz_file_url)
    csv_file = gz_file.replace('.gz', '')
    local_directory = os.path.join(local_storage_path, dir_name)
    os.makedirs(local_directory, exist_ok=True)

    subprocess.run(f'gsutil cp {gz_file_url} {local_directory}', shell=True)

    gz_file_path = os.path.join(local_directory, gz_file)
    subprocess.run(f'gzip -d {gz_file_path}', shell=True)

    csv_file_path = os.path.join(local_directory, csv_file)
    dest_file_url = f'{destination_bucket}/{dir_name}/{csv_file}'
    result = subprocess.run(f'gsutil cp {csv_file_path} {dest_file_url}', shell=True)

    if result.returncode == 0:
        print(f"Upload successful, deleting local file: {csv_file}")
        os.remove(csv_file_path)
    else:
        print(f"Upload failed for file: {csv_file}")

    os.rmdir(local_directory)

def main():
    source_bucket = 'gs://clusterdata-2011-2'
    destination_bucket = 'gs://large-data/data'
    local_storage_path = 'temp'

    dir_name = input("Enter the directory name: ")
    if not dir_name:
        print("Directory name is required")
        return

    dir_path = f'{source_bucket}/{dir_name}/'
    last_saved_number = get_last_saved_file_number(destination_bucket, dir_name)

    cmd = f'gsutil ls "{dir_path}" | grep ".gz$"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    source_files = result.stdout.splitlines()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for gz_file_url in source_files:
            gz_file = os.path.basename(gz_file_url)
            csv_file = gz_file.replace('.gz', '')
            file_number = extract_sequence_number(csv_file)

            if file_number <= last_saved_number:
                print(f"Skipping already processed file: {csv_file}")
                continue

            futures.append(executor.submit(process_file, gz_file_url, local_storage_path, destination_bucket, dir_name))

        # Wait for all submitted tasks to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == "__main__":
    main()
