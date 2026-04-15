#!/usr/bin/python3

import argparse
import datetime
import io
import json
import zipfile

import sqlalchemy


################################################################################
# helpers
################################################################################

def remove_nulls(s):
    """
    Postgres strings cannot contain the null byte.
    """
    if s is None:
        return None
    return s.replace('\x00', '')


def get_text(tweet):
    try:
        return tweet["extended_tweet"].get("full_text", tweet.get("text"))
    except KeyError:
        return tweet.get("text")


def get_entities(tweet):
    try:
        return tweet["extended_tweet"]["entities"]
    except KeyError:
        return tweet["entities"]


def get_country_code(tweet):
    place = tweet.get("place")
    if place is None:
        return None
    country_code = place.get("country_code")
    if country_code is None:
        return None
    return country_code.lower()


def get_state_code(tweet):
    place = tweet.get("place")
    if place is None:
        return None
    country_code = place.get("country_code")
    if country_code is None or country_code.lower() != "us":
        return None

    full_name = place.get("full_name")
    if full_name is None:
        return None

    state_code = full_name.split(",")[-1].strip().lower()
    if len(state_code) != 2:
        return None
    return state_code


def get_place_name(tweet):
    place = tweet.get("place")
    if place is None:
        return None
    return place.get("full_name")


def get_geo_wkt(tweet):
    """
    Returns WKT string for geo field, or None.
    """
    try:
        coords = tweet["geo"]["coordinates"]
        if coords is not None:
            return f"POINT({coords[0]} {coords[1]})"
    except (KeyError, TypeError):
        pass

    try:
        polys = tweet["place"]["bounding_box"]["coordinates"]
        if polys is None:
            return None

        parts = []
        for poly in polys:
            ring = []
            for point in poly:
                ring.append(f"{point[0]} {point[1]}")
            if poly and poly[0] != poly[-1]:
                ring.append(f"{poly[0][0]} {poly[0][1]}")
            parts.append(f"(({','.join(ring)}))")

        return f"MULTIPOLYGON({','.join(parts)})"
    except (KeyError, TypeError):
        return None


################################################################################
# main insert logic
################################################################################

def insert_tweet(connection, tweet):
    entities = get_entities(tweet)
    text = get_text(tweet)
    country_code = get_country_code(tweet)
    state_code = get_state_code(tweet)
    place_name = get_place_name(tweet)
    geo_wkt = get_geo_wkt(tweet)

    # -------------------------------------------------------------------------
    # users: hydrated sender
    # -------------------------------------------------------------------------
    user_url = tweet["user"].get("url")

    connection.execute(sqlalchemy.text("""
        INSERT INTO users (
            id_users,
            created_at,
            updated_at,
            friends_count,
            listed_count,
            favourites_count,
            statuses_count,
            protected,
            verified,
            screen_name,
            name,
            location,
            description,
            withheld_in_countries
        ) VALUES (
            :id_users,
            :created_at,
            :updated_at,
            :friends_count,
            :listed_count,
            :favourites_count,
            :statuses_count,
            :protected,
            :verified,
            :screen_name,
            :name,
            :location,
            :description,
            :withheld_in_countries
        )
    """), {
        "id_users": tweet["user"]["id"],
        "created_at": tweet["user"]["created_at"],
        "updated_at": tweet["created_at"],
        "friends_count": tweet["user"]["friends_count"],
        "listed_count": tweet["user"]["listed_count"],
        "favourites_count": tweet["user"]["favourites_count"],
        "statuses_count": tweet["user"]["statuses_count"],
        "protected": tweet["user"]["protected"],
        "verified": tweet["user"]["verified"],
        "screen_name": remove_nulls(tweet["user"]["screen_name"]),
        "name": remove_nulls(tweet["user"]["name"]),
        "location": remove_nulls(tweet["user"]["location"]),
        "description": remove_nulls(tweet["user"]["description"]),
        "withheld_in_countries": tweet["user"].get("withheld_in_countries"),
    })

    # -------------------------------------------------------------------------
    # users: unhydrated reply target
    # -------------------------------------------------------------------------
    if tweet.get("in_reply_to_user_id") is not None:
        connection.execute(sqlalchemy.text("""
            INSERT INTO users (
                id_users,
                screen_name,
                name
            ) VALUES (
                :id_users,
                :screen_name,
                :name
            )
        """), {
            "id_users": tweet["in_reply_to_user_id"],
            "screen_name": remove_nulls(tweet.get("in_reply_to_screen_name")),
            "name": None,
        })

    # -------------------------------------------------------------------------
    # users: unhydrated mentions
    # -------------------------------------------------------------------------
    for mention in entities.get("user_mentions", []):
        connection.execute(sqlalchemy.text("""
            INSERT INTO users (
                id_users,
                screen_name,
                name
            ) VALUES (
                :id_users,
                :screen_name,
                :name
            )
        """), {
            "id_users": mention["id"],
            "screen_name": remove_nulls(mention.get("screen_name")),
            "name": None,
        })

    # -------------------------------------------------------------------------
    # tweets
    # -------------------------------------------------------------------------
    connection.execute(sqlalchemy.text("""
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
        ) VALUES (
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
                WHEN :geo IS NULL THEN NULL
                ELSE ST_GeomFromText(:geo, 4326)
            END
        )
    """), {
        "id_tweets": tweet["id"],
        "id_users": tweet["user"]["id"],
        "created_at": tweet["created_at"],
        "in_reply_to_status_id": tweet.get("in_reply_to_status_id"),
        "in_reply_to_user_id": tweet.get("in_reply_to_user_id"),
        "quoted_status_id": tweet.get("quoted_status_id"),
        "retweet_count": tweet.get("retweet_count", 0),
        "favorite_count": tweet.get("favorite_count", 0),
        "quote_count": tweet.get("quote_count", 0),
        "withheld_copyright": tweet.get("withheld_copyright", False),
        "withheld_in_countries": tweet.get("withheld_in_countries"),
        "source": remove_nulls(tweet.get("source")),
        "text": remove_nulls(text),
        "country_code": country_code,
        "state_code": state_code,
        "lang": tweet.get("lang"),
        "place_name": remove_nulls(place_name),
        "geo": geo_wkt,
    })

    # -------------------------------------------------------------------------
    # tweet_urls
    # -------------------------------------------------------------------------
    for url in entities.get("urls", []):
        expanded_url = url.get("expanded_url") or url.get("url")
        if expanded_url is None:
            continue
        connection.execute(sqlalchemy.text("""
            INSERT INTO tweet_urls (
                id_tweets,
                url
            ) VALUES (
                :id_tweets,
                :url
            )
            ON CONFLICT DO NOTHING
         """), {
            "id_tweets": tweet["id"],
            "url": expanded_url
         })

    # -------------------------------------------------------------------------
    # tweet_mentions
    # -------------------------------------------------------------------------
    for mention in entities.get("user_mentions", []):
        connection.execute(sqlalchemy.text("""
            INSERT INTO tweet_mentions (
                id_tweets,
                id_users
            ) VALUES (
                :id_tweets,
                :id_users
            )
            ON CONFLICT DO NOTHING
        """), {
            "id_tweets": tweet["id"],
            "id_users": mention["id"],
        })

    # -------------------------------------------------------------------------
    # tweet_tags
    # IMPORTANT: lowercase tags so the provided SQL works
    # -------------------------------------------------------------------------
    hashtags = entities.get("hashtags", [])
    cashtags = entities.get("symbols", [])

    tags = (
        ["#" + hashtag["text"].lower() for hashtag in hashtags]
        + ["$" + cashtag["text"].lower() for cashtags in cashtags]
    )

    for tag in tags:
        connection.execute(sqlalchemy.text("""
            INSERT INTO tweet_tags (
                id_tweets,
                tag
            ) VALUES (
                :id_tweets,
                :tag
            )
            ON CONFLICT DO NOTHING
        """), {
            "id_tweets": tweet["id"],
            "tag": remove_nulls(tag),
        })

    # -------------------------------------------------------------------------
    # tweet_media
    # -------------------------------------------------------------------------
    media = []
    try:
        media = tweet["extended_tweet"]["extended_entities"]["media"]
    except KeyError:
        try:
            media = tweet["extended_entities"]["media"]
        except KeyError:
            media = []

    for medium in media:
        media_url = medium.get("media_url")
        if media_url is None:
            continue
        connection.execute(sqlalchemy.text("""
            INSERT INTO tweet_media (
                id_tweets,
                url,
                type
            ) VALUES (
                :id_tweets,
                :url,
                :type
            )
        """), {
            "id_tweets": tweet["id"],
            "url": media_url,
            "type": remove_nulls(medium.get("type")),
        })


################################################################################
# main
################################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--inputs", nargs="+", required=True)
    parser.add_argument("--print_every", type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(
        args.db,
        connect_args={"application_name": "load_tweets.py"},
    )

    with engine.begin() as connection:
        for filename in sorted(args.inputs, reverse=True):
            with zipfile.ZipFile(filename, "r") as archive:
                print(datetime.datetime.now(), filename)
                for subfilename in sorted(archive.namelist(), reverse=True):
                    if subfilename.endswith("/"):
                        continue

                    with archive.open(subfilename) as f:
                        for i, line in enumerate(f):
                            tweet = json.loads(line.decode("utf-8"))
                            insert_tweet(connection, tweet)

                            if i % args.print_every == 0:
                                print(
                                    datetime.datetime.now(),
                                    filename,
                                    subfilename,
                                    "i=",
                                    i,
                                    "id=",
                                    tweet["id"],
                                )
