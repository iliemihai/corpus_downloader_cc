#!/bin/bash

archives=(CC-MAIN-2023-14 CC-MAIN-2023-06 CC-MAIN-2022-49 CC-MAIN-2022-40 CC-MAIN-2021-39 CC-MAIN-2021-43 CC-MAIN-2021-49 CC-MAIN-2022-05 CC-MAIN-2022-21 CC-MAIN-2022-27 CC-MAIN-2022-33)

for CRAWL_ARCHIVE_ID in "${archives[@]}"
do
    echo https://data.commoncrawl.org/crawl-data/${CRAWL_ARCHIVE_ID}/wet.paths.gz
    wget -r --no-parent https://data.commoncrawl.org/crawl-data/${CRAWL_ARCHIVE_ID}/wet.paths.gz
done
