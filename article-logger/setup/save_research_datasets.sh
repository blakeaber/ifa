#!/bin/bash

# code to download Signal dataset, split, save to s3
# http://research.signalmedia.co/newsir16/
# ---------------------------------------------------------------------
wget http://research.signalmedia.co/newsir16/signalmedia-1m.jsonl.gz
gunzip signalmedia-1m.jsonl.gz
mkdir signal-split

split --numeric-suffixes --bytes=250MB --filter='gzip > $FILE.gz' ./signalmedia-1m.jsonl ./signal-split/
aws s3 cp ./signal-split/ s3://data-science-263198015083/data/signal-media/ --recursive


# code to download AG dataset, split, save to s3
# https://www.di.unipi.it/~gulli/AG_corpus_of_news_articles.html
# ---------------------------------------------------------------------
wget https://www.di.unipi.it/~gulli/newsSpace.bz2 --no-check-certificate
bzip2 -d newsSpace.bz2
mkdir ag-split

split --numeric-suffixes --bytes=250MB --filter='gzip > $FILE.gz' ./newsSpace ./ag-split/
aws s3 cp ./ag-split/ s3://data-science-263198015083/data/ag-research/ --recursive
