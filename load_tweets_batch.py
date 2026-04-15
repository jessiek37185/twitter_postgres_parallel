#!/usr/bin/python3

import sqlalchemy
import datetime
import zipfile
import io
import json

def remove_nulls(s):
    if s is None:
        return None
    return s.replace('\x00','\\x00')

def insert_tweets(connection, tweets):
    # Using dictionaries to deduplicate in Python since we are removing DB constraints
    unique_users = {}
    tweet_mentions = []
    tweets_data = []
    tweet_urls = []

    for tweet in tweets:
        user = tweet['user']
        # 1. Handle Users
        unique_users[user['id']] = {
            'id_users': user['id'],
            'screen_name': remove_nulls(user['screen_name']),
        }

        # 2. Handle Mentions
        for m in tweet['entities']['user_mentions']:
            if m['id'] not in unique_users:
                unique_users[m['id']] = {
                    'id_users': m['id'],
                    'screen_name': remove_nulls(m.get('screen_name'))
                }
            
            tweet_mentions.append({
                'id_tweets': tweet['id'],
                'id_users': m['id']
            })

        # 3. Handle URLs (Storing raw TEXT as per instructions)
        for url_obj in tweet['entities'].get('urls', []):
            tweet_urls.append({
                'id_tweets': tweet['id'],
                'url': url_obj.get('expanded_url') or url_obj.get('url')
            })

        # 4. Handle Tweet Data
        geo = None
        try:
            coords = tweet['geo']['coordinates']
            geo = f"POINT({coords[1]} {coords[0]})" # Standard Long/Lat order
        except:
            pass

        text = tweet.get('text')
        if 'extended_tweet' in tweet:
            text = tweet['extended_tweet'].get('full_text', text)

        tweets_data.append({
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet['created_at'],
            'text': remove_nulls(text),
            'geo': geo
        })

    # Start transaction
    with connection.begin():
        # Insert Users
        connection.execute(sqlalchemy.text('''
            INSERT INTO users (id_users, screen_name)
            VALUES (:id_users, :screen_name)
            ON CONFLICT DO NOTHING
        '''), list(unique_users.values()))

        # Insert Tweets
        connection.execute(sqlalchemy.text('''
            INSERT INTO tweets (id_tweets, id_users, created_at, text, geo)
            VALUES (
                :id_tweets, :id_users, :created_at, :text,
                CASE WHEN :geo IS NULL THEN NULL ELSE ST_GeomFromText(:geo, 4326) END
            )
            ON CONFLICT DO NOTHING
        '''), tweets_data)

        # Insert Mentions
        connection.execute(sqlalchemy.text('''
            INSERT INTO tweet_mentions (id_tweets, id_users)
            VALUES (:id_tweets, :id_users)
            ON CONFLICT DO NOTHING
        '''), tweet_mentions)

        # Insert URLs (Now using raw TEXT column 'url')
        if tweet_urls:
            connection.execute(sqlalchemy.text('''
                INSERT INTO tweet_urls (id_tweets, url)
                VALUES (:id_tweets, :url)
            '''), tweet_urls)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)
    connection = engine.connect()

    for filename in args.inputs:
        with zipfile.ZipFile(filename, 'r') as archive:
            for subfilename in archive.namelist():
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    tweets = [json.loads(line) for line in f]
                    insert_tweets(connection, tweets)
            tweet_mentions.append({
                'id_tweets': tweet['id'],
                'id_users': m['id']
            })

        # tweet
        geo = None
        try:
            coords = tweet['geo']['coordinates']
            geo = f"POINT({coords[0]} {coords[1]})"
        except:
            pass

        text = tweet.get('text')
        if 'extended_tweet' in tweet:
            text = tweet['extended_tweet'].get('full_text', text)

        tweets_data.append({
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet['created_at'],
            'text': remove_nulls(text),
            'geo': geo
        })

    with connection.begin():

        connection.execute(sqlalchemy.text('''
            INSERT INTO users (id_users, screen_name)
            VALUES (:id_users, :screen_name)
            ON CONFLICT (id_users) DO NOTHING
        '''), users)

        connection.execute(sqlalchemy.text('''
            INSERT INTO users (id_users, screen_name)
            VALUES (:id_users, :screen_name)
            ON CONFLICT (id_users) DO NOTHING
        '''), users_unhydrated)

        connection.execute(sqlalchemy.text('''
            INSERT INTO tweets (id_tweets, id_users, created_at, text, geo)
            VALUES (
                :id_tweets, :id_users, :created_at, :text,
                CASE WHEN :geo IS NULL THEN NULL ELSE ST_GeomFromText(:geo,4326) END
            )
            ON CONFLICT (id_tweets) DO NOTHING
        '''), tweets_data)

        connection.execute(sqlalchemy.text('''
            INSERT INTO tweet_mentions (id_tweets, id_users)
            VALUES (:id_tweets, :id_users)
            ON CONFLICT DO NOTHING
        '''), tweet_mentions)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)
    connection = engine.connect()

    for filename in args.inputs:
        with zipfile.ZipFile(filename, 'r') as archive:
            for subfilename in archive.namelist():
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    tweets = [json.loads(line) for line in f]
                    insert_tweets(connection, tweets)
