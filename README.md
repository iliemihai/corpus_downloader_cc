# Common Crawl Dataset Downloader

The Common Crawl dataset downloader is an open source repository of web crawl data that can be accessed and analyzed by anyone. This repo contains a simple script (run.sh) that automates the process of downloading the Romanian Common Crawl datasets.

## Prerequisites

Before you begin, ensure you have met the following requirements:

* You have a Unix-like environment (Linux distribution, Mac OS, or WSL on Windows)
* You have installed wget command-line utility
* You have stable internet connection with sufficient bandwidth (these files can be large!)

## Usage
### Clone the Repository

To download the Romanian Common Crawl dataset, you need to clone this repo to your local machine. Use the following command:

```bash
git clone https://github.com/iliemihai/corpus_downloader_cc
cd corpus_downloader_cc
```

To download the datasets, run the script in the terminal as follows:

```bash
python run.py
```
This script will download the Common Crawl datasets and store them in the data/ directory of the current repo.


## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
## License

This project uses the MIT License.
## Contact

If you want to contact me, you can reach me at ilie.mihai92@gmail.com.
## Disclaimer

Downloading large datasets can consume substantial bandwidth and disk space. Be aware of any associated costs or limitations with your internet service provider. It is your responsibility to manage your data storage and monitor your network usage.
## Acknowledgments

This script is powered by the data from Common Crawl. We appreciate the organization's commitment to maintaining an open repository of web crawl data.

