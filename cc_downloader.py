import subprocess
from concurrent.futures import ThreadPoolExecutor

# List of paths to wet files
wet_paths = [
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2021-39/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2021-49/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2022-21/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2022-49/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2021-43/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2022-27/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/wet.paths.gz",
        "./data.commoncrawl.org/crawl-data/CC-MAIN-2023-06/wet.paths.gz"
]

# Function to download a file
def download_wet_file(path):
    subprocess.run(["python3", "download_common_crawl_wet_files.py", "--wet-paths", path, "--output", "./out_docs"])

# Create a ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    # Use the executor to start a thread for each file download
    executor.map(download_wet_file, wet_paths)

