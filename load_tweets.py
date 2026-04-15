#!/usr/bin/python3

import sqlalchemy
import datetime
import zipfile
import io
import json


def remove_nulls(s):
    if s is None:
        return None
    return s.replace('\x00','')


def get_id_urls(url, connection):
    sql = sqlalchemy.sql.text('''
        INSERT INTO urls (url)
        VALUES (:url)
        ON CONFLICT (url) DO UPDATE SET url=EXCLUDED.url
        RETURNING id_urls;
    ''')
    res = connection.execute(sql, {'url': url}).first()
    return res[0]


def insert_tweet(connection, tweet):

    ########################################
    # USERS
    ########################################
    if tweet['user']['url'] is None:
        user_id_urls = None
    else:
        user_id_urls = get_id_urls(tweet['user']['url'], connection)

    sql = sqlalchemy.sql.text('''
        INSERT INTO users (
            id_users, created_at, updated_at, id_urls,
            friends_count, listed_count, favourites_count, statuses_count,
            protected, verified, screen_name, name, location,
            description, withheld_in_countries
        ) VALUES (
            :id_users, :created_at, :updated_at, :id_urls,
            :friends_count, :listed_count, :favourites_count, :statuses_count,
            :protected, :verified, :screen_name, :name, :location,
            :description, :withheld_in_countries
        )
        ON CONFLICT (id_users) DO NOTHING
    ''')

    connection.execute(sql, {
        'id_users': tweet['user']['id'],
        'created_at': tweet['user']['created_at'],
        'updated_at': tweet['user']['created_at'],
        'id_urls': user_id_urls,
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


    ########################################
    # GEO
    ########################################
    geo_str = None
    geo_coords = None

    try:
        coords = tweet['geo']['coordinates']
        geo_coords = f"{coords[0]} {coords[1]}"
        geo_str = 'POINT'
    except:
        pass


    ########################################
    # TEXT
    ########################################
    try:
        text = tweet['extended_tweet']['full_text']
    except:
        text = tweet['text']


    ########################################
    # TWEETS
    ########################################
    sql = sqlalchemy.sql.text('''
        INSERT INTO tweets (
            id_tweets, id_users, created_at,
            in_reply_to_status_id, in_reply_to_user_id, quoted_status_id,
            retweet_count, favorite_count, quote_count,
            withheld_copyright, withheld_in_countries,
            source, text, country_code, state_code, lang,
            place_name, geo
        ) VALUES (
            :id_tweets, :id_users, :created_at,
            :in_reply_to_status_id, :in_reply_to_user_id, :quoted_status_id,
            :retweet_count, :favorite_count, :quote_count,
            :withheld_copyright, :withheld_in_countries,
            :source, :text, :country_code, :state_code, :lang,
            :place_name,
            CASE WHEN :geo IS NULL THEN NULL ELSE ST_GeomFromText(:geo,4326) END
        )
        ON CONFLICT (id_tweets) DO NOTHING
    ''')

    connection.execute(sql, {
        'id_tweets': tweet['id'],
        'id_users': tweet['user']['id'],
        'created_at': tweet['created_at'],
        'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
        'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
        'quoted_status_id': tweet.get('quoted_status_id'),
        'retweet_count': tweet.get('retweet_count', 0),
        'favorite_count': tweet.get('favorite_count', 0),
        'quote_count': tweet.get('quote_count', 0),
        'withheld_copyright': tweet.get('withheld_copyright', False),
        'withheld_in_countries': tweet.get('withheld_in_countries'),
        'source': remove_nulls(tweet.get('source')),
        'text': remove_nulls(text),
        'country_code': None,
        'state_code': None,
        'lang': tweet.get('lang'),
        'place_name': None,
        'geo': f"{geo_str}({geo_coords})" if geo_str else None
    })


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)
    connection = engine.connect()

    for filename in sorted(args.inputs, reverse=True):
        with zipfile.ZipFile(filename, 'r') as archive:
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for line in f:
                        tweet = json.loads(line)
                        insert_tweet(connection, tweet)
