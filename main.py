import json
import db
import html
from db import (Comment as db_Comment, Submission as db_Submission)
import psycopg2
from argparse import ArgumentParser
import logging


filename = "F:\\RedditDump\\RS_2022-10"

db.create_tables()


def main():
    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="filename",
                        help="write report to FILE", metavar="FILE")
    try:
        connection = psycopg2.connect(user="push",
                                      password="123456",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="pushshift")

        cursor = connection.cursor()
        args = parser.parse_args()

        with open(args.filename, encoding="utf8") as f:
            query_count = 0
            for line in f:
                json_item = json.loads(line)
                # Do something with 'line'
                try:
                    if json_item['author'] == '[deleted]':
                        continue
                    if 'body' in json_item:
                        # if 'body' is present then assume it's a comment
                        id = json_item["id"]
                        # id = '123456'
                        cursor.execute("SELECT id FROM comment WHERE id = %s", (id,))
                        db_record = cursor.fetchall()

                        if not db_record:
                            json_item['body'] = clean_text(json_item['body'])

                            # Try to detect whether the comment is a URL only with no text so we can ignore it later
                            json_item['subreddit'] = clean_text(json_item['subreddit'])
                            json_item['permalink'] = clean_text(json_item['permalink'])
                            json_item['is_url_only'] = (json_item['body'].startswith('[') and json_item['body'].endswith(
                                ')')) \
                                                       or ('http' in json_item['body'].lower() and ' ' not in json_item[
                                'body'])
                            comment_keys = ('id', 'author', 'author_flair_text', 'body', 'created_utc', 'link_id',
                                            'parent_id', 'is_url_only', 'score', 'stickied')
                            comment_dict = {}
                            for i in json_item:
                                if i in comment_keys:
                                    comment_dict[i] = json_item[i]

                            keys = comment_dict.keys()
                            columns = ','.join(keys)
                            values = ','.join(['%({})s'.format(k) for k in keys])
                            insert = 'insert into comment ({0}) values ({1})'.format(columns, values)

                            # print(cursor.mogrify(insert, json_item))
                            cursor.execute((cursor.mogrify(insert, json_item)))
                            print(f"comment {json_item['id']} was ingested in database")
                        else:
                            print(f"comment {json_item['id']} already in database")

                    elif 'selftext' in json_item or 'url' in json_item:
                        # if 'selftext' is present then assume it's a submission
                        id = json_item["id"]
                        # id = '123456'
                        cursor.execute("SELECT id FROM submission WHERE id = %s", (id,))
                        db_record = cursor.fetchall()

                        if not db_record:
                            json_item['selftext'] = clean_text(json_item['selftext'])
                            json_item['url'] = clean_text(json_item['url'])
                            json_item['subreddit'] = clean_text(json_item['subreddit'])
                            json_item['permalink'] = clean_text(json_item['permalink'])
                            submission_keys = (
                            'id', 'author', 'author_flair_text', 'created_utc', 'is_self', 'num_comments',
                            'over_18','permalink', 'score', 'selftext', 'spoiler', 'subreddit', 'title', 'url')
                            submission_dict = {}
                            for i in json_item:
                                if i in submission_keys:
                                    if json_item.find('\x00'):
                                        json_item[i] = json_item[i].replace('\x00','')
                                    submission_dict[i] = json_item[i]

                            keys = submission_dict.keys()
                            columns = ','.join(keys)
                            values = ','.join(['%({})s'.format(k) for k in keys])
                            insert = 'insert into submission ({0}) values ({1})'.format(columns, values)

                            test = (cursor.mogrify(insert, submission_dict))
                            cursor.execute((cursor.mogrify(insert, submission_dict)))
                    # if verbose:
                    #    print(f"submission {json_item['id']} written to database")
                            print(f"submission {json_item['id']} was ingested in database")
                        else:
                            print(f"submission {json_item['id']} already in database")
                except Exception as e:
                    logger.error('Failed to upload to ftp: ' + str(e))
                if query_count >= 25:
                    connection.commit()
                    query_count = 0
                query_count = query_count + 1

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # if connection:
        #    cursor.close()
        #    connection.close()
        print("PostgreSQL connection is closed")


def clean_text(text):
    # have to unescape it twice, for reason I don't fully understand
    text = html.unescape(text)
    text = html.unescape(text)
    text = text.replace('\x00','')
    # Strip and whitespace off of the end
    text = text.strip()

    return text


def comment_to_db(comment):
    print(comment)


def submission_to_db(submission):
    print(submission)


if __name__ == '__main__':
    main()
