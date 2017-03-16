#!/bin/sh

# filename of the file to be checked i.e. feed_records
# do not include the file extension, it is assumed to be .txt
file=$1

# filesize ceiling of the file i.e. 250000 (250Mb)
maxsize=$2

# current unix timestamp to version the files
current_time=`date +%s`

# query the current file size
actualsize=$(du -k "./$file/$file.txt" | cut -f1)

# if file too large, gzip and send to s3
if [ $actualsize -ge $maxsize ]; then
    gzip ./$file/$file.txt
    aws s3 cp ./$file/$file.txt.gz s3://data-science-263198015083/crawler/$file/$current_time
    rm ./$file/$file.txt.gz
fi