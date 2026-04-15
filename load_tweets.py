#!/usr/bin/python3

import sqlalchemy
import zipfile
import io
import json


def remove_nulls(s):
    if s is None:
        return None
    return s.replace('\x00', '')


def get_id_urls(url, connection):
    sql = sqlalchemy.text('''
        INSERT INTO urls (url)
        VALUES (:url)
        ON CONFLICT (url) DO UPDATE SET url=EXCLUDED.url
        RETURNING id_urls;
    ''')
    res = connection.execute(sql, {'url': url}).first()
    return res[0]


def insert_tweet(connection, tweet):

    connection.execute(sqlalchemy.text('''
        INSERT INTO users (
            id_users, created_at, updated_at,
            friends_count, listed_count, favourites_count, statuses_count,
            protected, verified, screen_name, name, location,
            description, withheld_in_countries
        ) VALUES (
            :id_users, :created_at, :updated_at,
            :friends_count, :listed_count, :favourites_count, :statuses_count,
            :protected, :verified, :screen_name, :name, :location,
            :description, :withheld_in_countries
        )
    '''), {
        'id_users': tweet['user']['id'],
        'created_at': tweet['user']['created_at'],
        'updated_at': tweet['created_at'],
        'friends_count': tweet['user']['friends_count'],
        'listed_count': tweet['user']['listed_count'],
        'favourites_count': tweet['user']['favourites_count'],
        'statuses_count': tweet['user']['statuses_count'],
        'protected': tweet['user']['protected'],
        'verified': tweet['user']['verified'],
        'screen_name': remove_nulls(tweet['user']['screen_name']),
        'name': remove_nulls(tweet['user']['name']),
        'location': remove_nulls(tweet['user']['location']),
        'description': remove_nulls(tweet['user']['description']),
        'withheld_in_countries': tweet['user'].get('withheld_in_countries')
    })

    # ---------- mentions ----------
    for m in tweet['entities']['user_mentions']:
        connection.execute(sqlalchemy.text('''
            INSERT INTO users (id_users, screen_name)
            VALUES (:id_users, :screen_name)
        '''), {
            'id_users': m['id'],
            'screen_name': remove_nulls(m.get('screen_name'))
        })

    if tweet.get('in_reply_to_user_id') is not None:
        connection.execute(sqlalchemy.text('''
            INSERT INTO users (id_users)
            VALUES (:id_users)
        '''), {
            'id_users': tweet['in_reply_to_user_id']
        })

    # ---------- geo ----------
    geo = None
    try:
        coords = tweet['geo']['coordinates']
        geo = f"POINT({coords[0]} {coords[1]})"
    except:
        pass

    # ---------- text ----------
    text = tweet.get('text')
    if 'extended_tweet' in tweet:
        text = tweet['extended_tweet'].get('full_text', text)

    # ---------- tweets ----------
    connection.execute(sqlalchemy.text('''
        INSERT INTO tweets (
            id_tweets, id_users, created_at,
            in_reply_to_status_id, in_reply_to_user_id, quoted_status_id,
            retweet_count, favorite_count, quote_count,
            source, text, lang, geo
        ) VALUES (
            :id_tweets, :id_users, :created_at,
            :in_reply_to_status_id, :in_reply_to_user_id, :quoted_status_id,
            :retweet_count, :favorite_count, :quote_count,
            :source, :text, :lang,
            CASE WHEN :geo IS NULL THEN NULL ELSE ST_GeomFromText(:geo,4326) END
        )
    '''), {
        'id_tweets': tweet['id'],
        'id_users': tweet['user']['id'],
        'created_at': tweet['created_at'],
        'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
        'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
        'quoted_status_id': tweet.get('quoted_status_id'),
        'retweet_count': tweet.get('retweet_count', 0),
        'favorite_count': tweet.get('favorite_count', 0),
        'quote_count': tweet.get('quote_count', 0),
        'source': remove_nulls(tweet.get('source')),
        'text': remove_nulls(text),
        'lang': tweet.get('lang'),
        'geo': geo
    })

    # ---------- tweet_mentions ----------
    for m in tweet['entities']['user_mentions']:
        connection.execute(sqlalchemy.text('''
            INSERT INTO tweet_mentions (id_tweets, id_users)
            VALUES (:id_tweets, :id_users)
        '''), {
            'id_tweets': tweet['id'],
            'id_users': m['id']
        })


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+')
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)
    connection = engine.connect()

    for filename in sorted(args.inputs, reverse=True):
       with zipfile.ZipFile(filename, 'r') as archive:
        for subfilename in archive.namelist():
            if subfilename.endswith('/'):
                continue

            with io.TextIOWrapper(archive.open(subfilename)) as f:
                for line in f:
                    tweet = json.loads(line)
                    insert_tweet(connection, tweet)

connection.commit()
