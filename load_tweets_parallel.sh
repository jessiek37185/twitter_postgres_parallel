#!/bin/sh

files=$(find data/*)

echo '================================================================================'
echo 'load pg_denormalized (parallel)'
echo '================================================================================'
time echo "$files" | parallel -j 4 ./load_denormalized.sh {}

echo '================================================================================'
echo 'load pg_normalized (sequential)'
echo '================================================================================'
time echo "$files" | parallel -j 1 python3 load_tweets.py \
    --db=postgresql://postgres:pass@localhost:5439/postgres \
    --inputs {}

echo '================================================================================'
echo 'load pg_normalized_batch (parallel)'
echo '================================================================================'
time echo "$files" | parallel -j 4 python3 load_tweets_batch.py \
    --db=postgresql://postgres:pass@localhost:5440/postgres \
    --inputs {}
