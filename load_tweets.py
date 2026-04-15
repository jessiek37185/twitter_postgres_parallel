#!/usr/bin/python3

import sqlalchemy
import datetime
import zipfile
import io
import json


def remove_nulls(s):
    if s is None:
        return None
    return s.replace('\x00', '')


def clean_dict(d):
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [clean_dict(x) for x in d]
    elif isinstance(d, str):
        return d.replace('\x00', '')
    return d


def get_id_urls(url, connection):
    connection.execute(sqlalchemy.text('''
        INSERT INTO urls (url)
        VALUES (:url)
        ON CONFLICT DO NOTHING
    '''), {'url':url})
    
    res = connection.execute(sqlalchemy.text('''
        SELECT id_urls FROM urls WHERE url=:url
    '''), {'url':url}).first()

    return res[0]


def insert_tweet(connection, tweet):

    tweet = clean_dict(tweet)

    # skip duplicates
    if connection.execute(sqlalchemy.text('''
        SELECT 1 FROM tweets WHERE id_tweets=:id
    '''), {'id': tweet['id']}).first():
        return

    ########################################
    # USERS
    ########################################
    user_url = tweet['user'].get('url')
    user_id_urls = get_id_urls(user_url, connection) if user_url else None

    connection.execute(sqlalchemy.text('''
    INSERT INTO users (
        id_users, created_at, updated_at, id_urls,
        friends_count, listed_count, favourites_count,
        statuses_count, protected, verified,
        screen_name, name, location, description,
        withheld_in_countries
    )
    VALUES (
        :id_users, :created_at, :updated_at, :id_urls,
        :friends_count, :listed_count, :favourites_count,
        :statuses_count, :protected, :verified,
        :screen_name, :name, :location, :description,
        :withheld_in_countries
    )
    ON CONFLICT DO NOTHING
    '''), {
        'id_users': tweet['user']['id'],
        'created_at': tweet['user']['created_at'],
        'updated_at': tweet['user'].get('updated_at'),
        'id_urls': user_id_urls,
        'friends_count': tweet['user']['friends_count'],
        'listed_count': tweet['user']['listed_count'],
        'favourites_count': tweet['user']['favourites_count'],
        'statuses_count': tweet['user']['statuses_count'],
        'protected': tweet['user']['protected'],
        'verified': tweet['user']['verified'],
        'screen_name': tweet['user']['screen_name'],
        'name': tweet['user']['name'],
        'location': tweet['user']['location'],
        'description': tweet['user']['description'],
        'withheld_in_countries': tweet['user'].get('withheld_in_countries', [])
    })

    ########################################
    # TEXT + GEO + PLACE (FIXED)
    ########################################
    try:
        text = tweet['extended_tweet']['full_text']
    except:
        text = tweet.get('text')

    text = remove_nulls(text)

    # FIXED place bug
    place = tweet.get('place')
    if place:
        country_code = place.get('country_code')
        if country_code:
            country_code = country_code.lower()

        try:
            state_code = place['full_name'].split(',')[-1].strip().lower()
            if len(state_code) > 2:
                state_code = None
        except:
            state_code = None

        place_name = place.get('full_name')
    else:
        country_code = None
        state_code = None
        place_name = None

    # SAFE geo
    try:
        coords = tweet['geo']['coordinates']
        wkt = f"POINT({coords[0]} {coords[1]})"
    except:
        wkt = None

    ########################################
    # FK SAFETY
    ########################################
    if tweet.get('in_reply_to_user_id'):
        connection.execute(sqlalchemy.text('''
        INSERT INTO users (id_users)
        VALUES (:id)
        ON CONFLICT DO NOTHING
        '''), {'id': tweet['in_reply_to_user_id']})

    ########################################
    # INSERT TWEET
    ########################################
    connection.execute(sqlalchemy.text('''
    INSERT INTO tweets (
        id_tweets, id_users, created_at,
        in_reply_to_status_id, in_reply_to_user_id,
        quoted_status_id, retweet_count, favorite_count,
        quote_count, withheld_copyright,
        withheld_in_countries, source, text,
        country_code, state_code, lang,
        place_name, geo
    )
    VALUES (
        :id_tweets, :id_users, :created_at,
        :in_reply_to_status_id, :in_reply_to_user_id,
        :quoted_status_id, :retweet_count, :favorite_count,
        :quote_count, :withheld_copyright,
        :withheld_in_countries, :source, :text,
        :country_code, :state_code, :lang,
        :place_name,
        CASE WHEN :wkt IS NULL THEN NULL ELSE ST_GeomFromText(:wkt) END
    )
    ON CONFLICT DO NOTHING
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
        'withheld_copyright': tweet.get('withheld_copyright', False),
        'withheld_in_countries': tweet.get('withheld_in_countries', []),
        'source': tweet.get('source'),
        'text': text,
        'country_code': country_code,
        'state_code': state_code,
        'lang': tweet.get('lang'),
        'place_name': place_name,
        'wkt': wkt
    })


########################################
# MAIN
########################################

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--print_every', type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)

    with engine.begin() as connection:
        for filename in sorted(args.inputs, reverse=True):
            print(datetime.datetime.now(), filename)

            with zipfile.ZipFile(filename, 'r') as archive:
                for subfilename in sorted(archive.namelist(), reverse=True):
                    with io.TextIOWrapper(archive.open(subfilename)) as f:
                        for i, line in enumerate(f):

                            tweet = json.loads(line)
                            insert_tweet(connection, tweet)

                            if i % args.print_every == 0:
                                print(datetime.datetime.now(), filename, subfilename, 'i=', i)
