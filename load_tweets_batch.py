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


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def insert_tweets(connection, tweets, batch_size=1000):
    for i, tweet_batch in enumerate(batch(tweets, batch_size)):
        print(datetime.datetime.now(), 'batch', i)
        _insert_tweets(connection, tweet_batch)


def _insert_tweets(connection, input_tweets):

    users = []
    tweets = []
    tweet_mentions = []
    tweet_tags = []
    tweet_media = []

    ########################################
    # STEP 1: build lists
    ########################################
    for tweet in input_tweets:

        users.append({
            'id_users': tweet['user']['id'],
            'created_at': tweet['user']['created_at'],
            'updated_at': tweet['created_at'],
            'screen_name': remove_nulls(tweet['user']['screen_name']),
            'name': remove_nulls(tweet['user']['name']),
            'location': remove_nulls(tweet['user']['location']),
            'description': remove_nulls(tweet['user']['description']),
            'protected': tweet['user']['protected'],
            'verified': tweet['user']['verified'],
            'friends_count': tweet['user']['friends_count'],
            'listed_count': tweet['user']['listed_count'],
            'favourites_count': tweet['user']['favourites_count'],
            'statuses_count': tweet['user']['statuses_count'],
            'withheld_in_countries': tweet['user'].get('withheld_in_countries'),
        })

        # GEO SAFE
        geo_str = None
        geo_coords = None
        try:
            coords = tweet['geo']['coordinates']
            geo_coords = f"{coords[0]} {coords[1]}"
            geo_str = 'POINT'
        except:
            pass

        try:
            text = tweet['extended_tweet']['full_text']
        except:
            text = tweet['text']

        tweets.append({
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet['created_at'],
            'geo_str': geo_str,
            'geo_coords': geo_coords,
            'text': remove_nulls(text),
            'lang': tweet.get('lang'),
            'source': remove_nulls(tweet.get('source')),
        })

        # mentions
        for mention in tweet['entities']['user_mentions']:
            tweet_mentions.append({
                'id_tweets': tweet['id'],
                'id_users': mention['id']
            })

        # tags
        for tag in tweet['entities']['hashtags']:
            tweet_tags.append({
                'id_tweets': tweet['id'],
                'tag': '#' + tag['text']
            })

    ########################################
    # STEP 2: insert (ONE transaction only)
    ########################################
    with connection.begin():

        if users:
            connection.execute(sqlalchemy.text('''
                INSERT INTO users (id_users, created_at, updated_at,
                    screen_name, name, location, description,
                    protected, verified, friends_count,
                    listed_count, favourites_count, statuses_count,
                    withheld_in_countries)
                VALUES (:id_users, :created_at, :updated_at,
                    :screen_name, :name, :location, :description,
                    :protected, :verified, :friends_count,
                    :listed_count, :favourites_count, :statuses_count,
                    :withheld_in_countries)
                ON CONFLICT (id_users) DO NOTHING
            '''), users)

        if tweets:
            sql = '''
                INSERT INTO tweets
                (id_tweets,id_users,created_at,geo,text,lang,source)
                VALUES
            '''
            values = []
            binds = {}

            for i, t in enumerate(tweets):
                values.append(f"""(
                    :id_tweets{i},
                    :id_users{i},
                    :created_at{i},
                    CASE WHEN :geo_str{i} IS NULL THEN NULL
                        ELSE ST_GeomFromText(:geo_str{i} || '(' || :geo_coords{i} || ')',4326)
                    END,
                    :text{i},
                    :lang{i},
                    :source{i}
                )""")

                for k, v in t.items():
                    binds[f"{k}{i}"] = v

            sql += ",".join(values) + " ON CONFLICT (id_tweets) DO NOTHING"

            connection.execute(sqlalchemy.text(sql), binds)

        if tweet_mentions:
            connection.execute(sqlalchemy.text('''
                INSERT INTO tweet_mentions (id_tweets,id_users)
                VALUES (:id_tweets,:id_users)
                ON CONFLICT DO NOTHING
            '''), tweet_mentions)

        if tweet_tags:
            connection.execute(sqlalchemy.text('''
                INSERT INTO tweet_tags (id_tweets,tag)
                VALUES (:id_tweets,:tag)
                ON CONFLICT DO NOTHING
            '''), tweet_tags)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--batch_size', type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)
    connection = engine.connect()

    for filename in sorted(args.inputs, reverse=True):
        with zipfile.ZipFile(filename, 'r') as archive:
            print(datetime.datetime.now(), filename)
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    tweets = [json.loads(line) for line in f]
                    insert_tweets(connection, tweets, args.batch_size)
