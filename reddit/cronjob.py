"""

cache.db contains all comments made in the past 2 weeks.

This script runs once every 10 minutes (600 seconds).

All 3 subreddits together (very roughly) generate 1 comment every minute. In the
worst case we perform 3 queries per comment (t=0, t=2d, t=14d) which means we'll
average 3 queries every minute (plus 3 every 10 minutes to find new comments).

1) Fetches the 100 newest comments from each subreddit and
   inserts them into cache.db

2) Finds all comments 2 or 14 days old and update them

3) Finds all comments over 2 weeks old and removes them
"""


# TODO: insert posts!

# TODO: don't use cache.db.  Just use the spot database.

"""
comments:
  comment_id, create_utc, json

tokens:

"""

import code, json, os, random, requests, sqlite3, time

from reddit import Reddit, create_submission

import spot
from utils import *

# Script runs every 10 minutes.
kCronjobTimestep = 10 * 60

kSecsPerDay = 60 * 60 * 24

def is_thread(comment):
  return 'title' in comment

def compute_depth(index, comment):
  depth = 0
  while not is_thread(comment):
    try:
      comment = index.json_from_docid(int(comment['parent_id'][3:], 36))
    except:
      code.interact(local=locals())
    depth += 1
    if comment is None:
      break
  return depth

def prep_comment_for_insertion(index, comment):
  docid = int(comment['id'], 36)
  parent_id = comment['parent_id'][3:]
  thread_id = comment['permalink'].split('/')[4]

  oldcomment = index.json_from_docid(docid)
  parent = index.json_from_docid(int(parent_id, 36))
  thread = index.json_from_docid(int(thread_id, 36))

  comment['random'] = random.random()

  comment['depth'] = compute_depth(index, comment)
  if is_thread(comment):
    assert comment['depth'] == 0
  else:
    assert comment['depth'] > 0

  tokens = get_tokens(comment, parent, thread, isthread=is_thread(comment))
  comment['tokens'] = ' '.join(tokens)

  return comment

if __name__ == '__main__':
  index = spot.Index('reddit/spot-index')

  reddit = Reddit()
  limit = 100

  # Step 1: fetch the 100 newest comments from each subreddit.
  # for subreddit in ['slatestarcodex', 'TheMotte', 'theschism']:
  for subreddit in ['theschism']:
    T = []
    r = reddit.request(
      f"https://www.reddit.com/r/{subreddit}/comments.json?limit={limit}")
    assert r['kind'] == 'Listing'
    comments = r['data']['children']

    # Iterate through comments from old to new so parents are guaranteed to be
    # inserted first.
    for comment in comments[::-1]:
      if comment['kind'] != 't1':
        print(f'WARNING: Unrecognized comment kind "{comment["kind"]}"')
      comment = comment['data']

      parts = comment['permalink'].split('/')
      assert parts[0] == ''
      assert parts[1] == 'r'
      assert parts[2] == subreddit
      assert parts[3] == 'comments'
      post_id = parts[4]
      comment_id = parts[6]

      comment = prep_comment_for_insertion(index, comment)
      # We use 'replace' here so when we insert a comment twice (which is
      # expected) we don't throw an error.
      index.replace(
        int(comment['id'], 36),
        comment['created_utc'],
        comment['tokens'].split(' '),
        comment
      )

  index.commit()

  # Step 2: for all comments either 2 days or 14 days old, refresh.
  # In practice this means finding all *posts* with comments that are 2 or 14
  # days old and fetching from them.
  for days in [2, 14]:
    a = time.time() - kSecsPerDay * days - kCronjobTimestep
    b = time.time() - kSecsPerDay * days
    index.c.execute(f'SELECT docid, json FROM documents WHERE created_utc > {a} AND created_utc < {b}')
    comments = index.c.fetchall()

    for docid, comment in comments:
      oldcomment = json.loads(comment)
      permalink = oldcomment['permalink']
      r = reddit.request(f"https://www.reddit.com{permalink[:-1]}.json")
      comment = r[1]['data']['children'][0]['data']
      if int(comment['id'], 36) != docid:
        print('Error (mismatching IDs)')
        exit(0)
        continue
      if comment['body'] != '[deleted]':
        comment = prep_comment_for_insertion(index, comment)
        index.replace(
          docid,
          comment['created_utc'],
          comment['tokens'].split(' '),
          comment
        )

  index.commit()

  # Step 3: comments



