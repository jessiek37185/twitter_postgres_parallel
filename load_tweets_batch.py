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

    users = []
    users_unhydrated = []
    tweet_mentions = []
    tweets_data = []

    for tweet in tweets:

        # full user
        users.append({
            'id_users': tweet['user']['id'],
            'screen_name': remove_nulls(tweet['user']['screen_name']),
        })

        # mentions (fix FK issue)
        for m in tweet['entities']['user_mentions']:
            users_unhydrated.append({
                'id_users': m['id'],
                'screen_name': remove_nulls(m.get('screen_name'))
            })

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
