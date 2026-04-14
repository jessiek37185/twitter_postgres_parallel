#!/bin/sh

# list all of the files that will be loaded into the database
# for the first part of this assignment, we will only load a small test zip file with ~10000 tweets
# but we will write are code so that we can easily load an arbitrary number of files
files='
test-data.zip
'

echo 'load normalized'
for file in $files; do
    # call the load_tweets.py file to load data into pg_normalized
    python3 load_tweets.py \
        --db postgresql://postgres:pass@localhost:5439/postgres \
        --inputs "$file" \
        --print_every 10000
done

