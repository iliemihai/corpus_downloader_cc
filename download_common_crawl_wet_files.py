import argparse
import json
import gzip
import io
import logging
import time
import os

from ftlangdetect import detect
import requests
from tqdm import tqdm

CC_DOMAIN = "https://data.commoncrawl.org"

# WET file constants
_PAGE_DELIMITER = "WARC/1.0"
_URL_KEY = "WARC-Target-URI:"
_URL_DATE = "WARC-Date:"
_CONTENT_TYPE = "Content-Type:"
_CONTENT_LANGUAGE = "WARC-Identified-Content-Language:"
_METADATA_PREFIXES = ("WARC", "CONTENT-", "Content-")


def check_if_gz_file_corrupted(gz_file):
    chunksize = 10 * 1024 ** 2

    with gzip.open(gz_file, 'rb') as f:
        try:
            while f.read(chunksize) != b'':
                pass
            return False
        except:
            return True

def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return test_f.read(2) == b'\x1f\x8b'


def split_wet_file(wet_file_path):
    def _validate_features(page):
        feature_list = ["url", "text", "timestamp"]
        for feature_name in feature_list:
            if feature_name not in page:
                return False

        return True

    page = dict()
    print("PATH ISL: ", wet_file_path)
    if is_gz_file(wet_file_path):
        file_wet_in = gzip.open(wet_file_path, "rt")
    else:
        file_wet_in = open(wet_file_path, "r")

    for i, line in enumerate(file_wet_in):
        line = line.strip()
        if not line:
            continue

        if line == _PAGE_DELIMITER:
            if i > 0 and _validate_features(page):
                yield page
            page = dict()

        if line.startswith(_URL_KEY):
            page["url"] = line[len(_URL_KEY):].strip()

        if line.startswith(_URL_DATE):
            page["timestamp"] = line[len(_URL_DATE):].strip()

        if line.startswith(_CONTENT_TYPE):
            page["content_type"] = line[len(_CONTENT_TYPE):].strip()

        if line.startswith(_CONTENT_LANGUAGE):
            page["content_language"] = line[len(_CONTENT_LANGUAGE):].strip()

        if line.startswith(_METADATA_PREFIXES):
            continue

        if "text" in page:
            page["text"] += "\n"
        page["text"] = page.get("text", "") + line

    if _validate_features(page):
        yield page


def download(*args, **kwargs):
    fname=kwargs["url"].split("/")[-1]
    chunk_size=1024
    resp = requests.get(*args, **kwargs, stream=True, timeout=3600)
    total = int(resp.headers.get('content-length', 0))
    with open(fname, 'wb') as file, tqdm(
        desc=fname,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)
    return resp

def request_with_retry(connection_reset_retry=20, *args, **kwargs):
    retries = 0
    while True:
        try:
            #response = requests.get(*args, **kwargs, timeout=3600)
            response = download(*args, **kwargs)
            return response
        except (
            ConnectionResetError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ):  
            if retries >= connection_reset_retry:
                logging.info(f"{args}")
                raise
            time.sleep(2 ** retries)
            retries += 1

def download_and_package(
    cc_path,
    out_file,
    romanian_filtering=True,
):
    logging.basicConfig(level=logging.DEBUG)
    for _ in range(10):
        response = request_with_retry(url=f"{CC_DOMAIN}/{cc_path}")
        #download_file = io.BytesIO(response.content)
        download_file = cc_path.split("/")[-1]
        page_list = []
        try:
            for page in tqdm(split_wet_file(download_file), desc=f"split_wet_file {download_file}"):
                if romanian_filtering:
                    if "content_language" not in page:
                        try:
                            language = detect(text=page["text"].replace("\n", " "), low_memory=False)["lang"]
                        except langdetect.lang_detect_exception.LangDetectException:
                            continue
                        if language not in ["ro"]:
                            continue
                    elif "ro" not in page["content_language"].split(","):
                        continue
                    elif "ron" not in page["content_language"].split(","):
                        continue
                    elif "rom" not in page["content_language"].split(","):
                        continue
                page_list.append(page)
            break
        except (EOFError, gzip.BadGzipFile):
            continue

    f = open(out_file, "w")
    for page in page_list:
        f.write(str(page)+"/n")


def read_wet_paths_file(filepath):
    for line in gzip.open(filepath, "rt"):
        cc_path = line.strip()
        yield cc_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wet-paths", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    cc_paths = []
    for wet_path in args.wet_paths:
        for cc_path in read_wet_paths_file(wet_path):
            cc_paths.append(cc_path)

    for i, path in enumerate(cc_paths):
        batch_size = 1 
        input = cc_paths[i * batch_size: (i + 1) * batch_size]
        out_file = os.path.join(args.output.strip(), path.replace(".gz", ".txt").split("/")[-1])
        download_and_package(input[0], out_file)
        print("Removing {0} ...".format(path.split("/")[5]))
        os.remove(path.split("/")[5])


if __name__ == "__main__":
    main()
