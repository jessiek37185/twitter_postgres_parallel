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


def extract_basic_fields(tweet):
    tweet = clean_dict(tweet)

    # TEXT
    try:
        text = tweet['extended_tweet']['full_text']
    except:
        text = tweet.get('text')
    text = remove_nulls(text)

    # PLACE (FIXED BUG)
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

    # GEO → SAFE
    try:
        coords = tweet['geo']['coordinates']
        wkt = f"POINT({coords[0]} {coords[1]})"
    except:
        wkt = None

    return {
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
        'wkt': wkt,
    }


def insert_tweets(connection, tweets):

    sql = sqlalchemy.text('''
    INSERT INTO tweets (
        id_tweets,
        id_users,
        created_at,
        in_reply_to_status_id,
        in_reply_to_user_id,
        quoted_status_id,
        retweet_count,
        favorite_count,
        quote_count,
        withheld_copyright,
        withheld_in_countries,
        source,
        text,
        country_code,
        state_code,
        lang,
        place_name,
        geo
    )
    VALUES (
        :id_tweets,
        :id_users,
        :created_at,
        :in_reply_to_status_id,
        :in_reply_to_user_id,
        :quoted_status_id,
        :retweet_count,
        :favorite_count,
        :quote_count,
        :withheld_copyright,
        :withheld_in_countries,
        :source,
        :text,
        :country_code,
        :state_code,
        :lang,
        :place_name,
        CASE 
            WHEN :wkt IS NULL THEN NULL 
            ELSE ST_GeomFromText(:wkt) 
        END
    )
    ON CONFLICT DO NOTHING
    ''')

    connection.execute(sql, tweets)


########################################
# MAIN
########################################

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--batch_size', type=int, default=1000)
    parser.add_argument('--print_every', type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db)

    with engine.begin() as connection:
        for filename in sorted(args.inputs, reverse=True):
            print(datetime.datetime.now(), filename)

            with zipfile.ZipFile(filename, 'r') as archive:
                for subfilename in sorted(archive.namelist(), reverse=True):
                    with io.TextIOWrapper(archive.open(subfilename)) as f:

                        batch = []
                        for i, line in enumerate(f):
                            tweet = json.loads(line)

                            row = extract_basic_fields(tweet)
                            batch.append(row)

                            if len(batch) >= args.batch_size:
                                insert_tweets(connection, batch)
                                batch = []

                            if i % args.print_every == 0:
                                print(datetime.datetime.now(), filename, 'batch', i)

                        # insert remaining
                        if batch:
                            insert_tweets(connection, batch)
