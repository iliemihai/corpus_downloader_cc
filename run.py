import subprocess

subprocess.run(["bash", "get_wet_archives.sh"])
subprocess.run(["python3", "downloader.py"])
