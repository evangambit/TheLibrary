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

import argparse, code, json, os, random, requests, sqlite3, time

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
  oldcomment = index.json_from_docid(docid)

  if 'parent_id' in comment:
    parent = index.json_from_docid(int(comment['parent_id'][3:], 36))
  else:
    parent = None

  thread_id = comment['permalink'].split('/')[4]
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
  parser = argparse.ArgumentParser(description='Grab and refresh recent comments')
  parser.add_argument('--num', '-n', type=int, default=100, help='Number of posts/comments to get')
  parser.add_argument('--outdir', '-o', type=str, required=True, help='Directory to dump jsons to')
  parser.add_argument('--subs', '-s', type=str, required=False, default='TheMotte,slatestarcodex,theschism', help='Comma-delimited list of subreddits')
  args = parser.parse_args()

  index = spot.Index('reddit/spot-index')

  reddit = Reddit()
  limit = 100

  # Step 1: fetch the 100 newest comments from each subreddit.
  # for subreddit in ['slatestarcodex', 'TheMotte', 'theschism']:
  for subreddit in args.subs.split(','):
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
      docid = int(comment['id'], 36)
      index.replace(
        int(comment['id'], 36),
        post_id,
        comment['created_utc'],
        comment['tokens'].split(' '),
        comment
      )

  index.commit()

  # Step 2: for all comments either 2 days or 14 days old, refresh.
  # In practice this means finding all *posts* with comments that are 2 or 14
  # days old and fetching from them.
  for days in [2, 14]:
    a = time.time() - kSecsPerDay * days - kCronjobTimestep * 2
    b = time.time() - kSecsPerDay * days
    index.c.execute(f'SELECT docid, postid, json FROM documents WHERE created_utc > {a} AND created_utc < {b}')
    comments = index.c.fetchall()
    print(f'refreshing {len(comments)} comments that are {days} days old')

    for docid, postid, oldcomment in comments:
      oldcomment = json.loads(oldcomment)
      permalink = oldcomment['permalink']
      r = reddit.request(f"https://www.reddit.com{permalink[:-1]}.json")
      if is_thread(oldcomment):
        comment = r[0]['data']['children'][0]['data']
      else:
        if len(r[1]['data']['children']) == 0:
          # Missing comment (often deleted).
          continue
        comment = r[1]['data']['children'][0]['data']
      if int(comment['id'], 36) != docid:
        print('Error (mismatching IDs)')
        exit(0)
        continue
      if comment['author'] != '[deleted]':
        comment = prep_comment_for_insertion(index, comment)
        index.replace(
          docid,
          postid,
          comment['created_utc'],
          comment['tokens'].split(' '),
          comment
        )

  # Dump threads into comments.
  for docid, postid, comment in comments:
    index.c.execute(f'SELECT json FROM documents WHERE postid == {postid}')
    C = [json.loads(c[0]) for c in index.c.fetchall()]
    post = [c for c in C if int(c['id'], 36) == postid]
    assert len(post) == 1
    post = post[0]

    C = [c for c in C if int(c['id'], 36) != postid]

    post['comments'] = C

    # TODO: compute year

    with open(pjoin(args.outdir, year, post['id'] + '.json'), 'w') as f:
      json.dump(post, f)

    break

  index.commit()

  # Step 3: comments


