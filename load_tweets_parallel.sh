#!/bin/sh

files=$(ls data/*.zip)

echo '================================================================================'
echo 'load pg_denormalized (parallel)'
echo '================================================================================'
# FIXME: implement this with GNU parallel
time echo "$files" | parallel ./load_denormalized.sh

echo '================================================================================'
echo 'load pg_normalized (parallel)'
echo '================================================================================'
# FIXME: implement this with GNU parallel
time echo "$files" | parallel python3 load_tweets.py \
    --db postgresql://postgres:pass@localhost:5439/postgres \ 
    --inputs {}


echo '================================================================================'
echo 'load pg_normalized_batch (parallel)'
echo '================================================================================'
# FIXME: implement this with GNU parallel
time echo "$files" | parallel python3 load_tweets_batch.py \ 
    --db postgresql://postgres:pass@localhost:5440/postgres \ 
    --inputs {}
