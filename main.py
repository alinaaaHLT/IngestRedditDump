import json
import db
import html
from db import (Comment as db_Comment, Submission as db_Submission)

filename = "F:\\RedditDump\\RC_2022-10"
db.create_tables()


def main():
    with open(filename, encoding="utf8") as f:
        i = 0
        for line in f:
            json_item = json.loads(line)
            # Do something with 'line'
            if json_item['author'] == '[deleted]':
                continue
            if 'body' in json_item:
                # if 'body' is present then assume it's a comment

                db_record = db_Comment.get_or_none(db_Comment.id == json_item['id'])

                if not db_record:
                    json_item['body'] = clean_text(json_item['body'])

                    # Try to detect whether the comment is a URL only with no text so we can ignore it later
                    json_item['is_url_only'] = (json_item['body'].startswith('[') and json_item['body'].endswith(')')) \
                                               or ('http' in json_item['body'].lower() and ' ' not in json_item['body'])
                    json_item['subreddit'] = clean_text(json_item['subreddit'])
                    json_item['permalink'] = clean_text(json_item['permalink'])
                    db_record = db_Comment.create(**json_item)
            # if verbose:
            #	print(f"comment {json_item['id']} written to database")

            elif 'selftext' in json_item:
                # if 'selftext' is present then assume it's a submission
                db_record = db_Submission.get_or_none(db_Submission.id == json_item['id'])

                if not db_record:
                    json_item['selftext'] = clean_text(json_item['selftext'])
                    json_item['subreddit'] = clean_text(json_item['subreddit'])
                    json_item['permalink'] = clean_text(json_item['permalink'])
                    db_record = db_Submission.create(**json_item)
        # if verbose:
        #    print(f"submission {json_item['id']} written to database")


            print(json_item['id'])


def clean_text(text):
    # have to unescape it twice, for reason I don't fully understand
    text = html.unescape(text)
    text = html.unescape(text)
    # Strip and whitespace off of the end
    text = text.strip()

    return text


if __name__ == '__main__':
    main()
