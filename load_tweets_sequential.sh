#!/bin/bash

set -e
files="data/*.zip"

echo '================================================================================'
echo 'load denormalized'
echo '================================================================================'
start=$(date +%s)
time for file in $files; do
    echo
	unzip -p "$file" | sed 's/\\u0000//g' | iconv -f utf-8 -t utf-8 -c | psql "postgresql://postgres:pass@localhost:5438" -c "COPY tweets_jsonb (data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
done
end=$(date +%s)
echo "Denormalized load elapsed: $((end - start))s"

echo '================================================================================'
echo 'load pg_normalized'
echo '================================================================================'
start=$(date +%s)
time for file in $files; do
    echo
    python3 load_tweets_batch.py --db "postgresql://postgres:pass@localhost:5439/postgres" --inputs "$file" 
done
end=$(date +%s)
echo "Normalized load elapsed: $((end - start))s"

echo '================================================================================'
echo 'load pg_normalized_batch'
echo '================================================================================'
start=$(date +%s)
time for file in $files; do
    echo
	python3 -u load_tweets_batch.py --db=postgresql://postgres:pass@localhost:5440/postgres --inputs "$file"
done
end=$(date +%s)
echo "Normalized load elapsed: $((end - start))s"
