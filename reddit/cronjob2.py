import argparse, code, json, os, random, requests, sqlite3, time
from datetime import datetime

from reddit import Reddit, create_submission

from utils import *

# Script runs every 10 minutes, but we set this value to
# 20 minutes so that there is some overlap.
kCronjobTimestep = 20 * 60

kSecsPerDay = 60 * 60 * 24

def is_thread(comment):
  return 'title' in comment

def get_post_fn(args, year, postid):
  return pjoin(args.outdir, str(year), postid + '.json')

def timestamp_to_year(timestamp_seconds):
  return datetime.utcfromtimestamp(timestamp_seconds).year

if __name__ == '__main__':
  print('========' * 4)
  print(f'Starting cronjob.py at {round(time.time())}s ({datetime.fromtimestamp(round(time.time()))})')

  parser = argparse.ArgumentParser(description='Grab and refresh recent comments')
  parser.add_argument('--num', '-n', type=int, default=100, help='Number of posts/comments to get')
  parser.add_argument('--outdir', '-o', type=str, required=True, help='Directory to dump jsons to')
  parser.add_argument('--subs', '-s', type=str, required=False, default='TheMotte,slatestarcodex,theschism', help='Comma-delimited list of subreddits')
  parser.add_argument('--indexpath', '-ip', type=str, required=False, default='reddit/spot-index', help='Location of spot index')
  args = parser.parse_args()

  reddit = Reddit()

  # Step 1: fetch the 100 newest comments from each subreddit.
  # for subreddit in ['slatestarcodex', 'TheMotte', 'theschism']:
  for subreddit in args.subs.split(','):
    T = []

    # Fetch new posts.
    newPosts = reddit.request(
      f"https://www.reddit.com/r/{subreddit}/new.json?sort=new"
    )['data']['children']
    newPosts = [c['data'] for c in newPosts if c['kind'] == 't3']

    # Fetch new comments.
    newComments = reddit.request(
      f"https://www.reddit.com/r/{subreddit}/comments.json?limit={args.num}"
    )['data']['children']
    newComments = [c['data'] for c in newComments if c['kind'] == 't1']

    oldest_comment_time = min(c['created_utc'] for c in newComments)
    print(f'Fetched {subreddit} comments back to %.2f hours ago'
      % ((time.time() - oldest_comment_time) / 3600)
    )

    # We'll keep track of what posts we need to write out to disk here.
    modifiedPosts = set()

    # For posts that already exist, load comments from JSON.
    for i, post in enumerate(newPosts):
      year = timestamp_to_year(post['created_utc'])
      fn = get_post_fn(args, year, post['id'])
      if os.path.exists(fn):
        with open(fn, 'r') as f:
          oldPost = json.load(f)
        if post['author'] == '[deleted]':
          newPosts[i] = oldPost
          continue
        if "comments" in oldPost:
          post["comments"] = oldPost["comments"]
          modifiedPosts.add(post['id'])
      else:
        modifiedPosts.add(post['id'])

    # If we've never seen a post before we quickly grab whatever comments we may
    # have missed.  In theory this isn't typicalyl necessary, but in practice
    # it's nice to know we can't do any worse than the original refresh script (
    # even if, e.g., too many comments come in in a time step).  If all goes
    # according to plan this should be fast, since posts should have no (or very
    # few) comments here.
    for i, post in enumerate(newPosts):
      if 'comments' in post:
        continue
      if post['created_utc'] < oldest_comment_time:
        print(f'Downloading all comments from {post["permalink"]}')
        newPosts[i] = create_submission(reddit, post['id']).json


    # Create map of posts
    posts = {}
    for post in newPosts:
      posts[post['id']] = post

      # If we've never seen this post before, we need to do a best effort to
      # fetch all of its comments.
      if 'comments' not in post:
        posts[postid] = create_submission(reddit, postid).json

    for c in newComments:
      _, _, _, _, postid, _, commentid, _ = c['permalink'].split('/')

      # Fetch post from disk if necessary
      if postid not in posts:
        year = timestamp_to_year(c['created_utc'])
        fn = get_post_fn(args, year, postid)
        # It's possible the comment belongs to a different year.
        if not os.path.exists(fn):
          fn = get_post_fn(args, year - 1, postid)
        # We haven't grabbed this post yet.
        if os.path.exists(fn):
          with open(fn, 'r') as f:
            posts[postid] = json.load(f)
        else:
          posts[postid] = create_submission(reddit, postid).json

      post = posts[postid]

      cids = [comment['id'] for comment in post['comments']]

      if c['id'] in cids:
        idx = cids.indexOf(c['id'])
        oldc = post['comments'][idx]
        if c['author'] == '[deleted]':
          continue
        post['comments'][idx] = c
        modifiedPosts.add(postid)
      else:
        post['comments'].append(c)
        modifiedPosts.add(postid)

    for postid in modifiedPosts:
      post = posts[postid]
      year = timestamp_to_year(post['created_utc'])
      fn = get_post_fn(args, year, postid)
      os.makedirs(os.path.split(fn)[0], exist_ok=True)
      with open(fn, 'w+') as f:
        json.dump(post, f, indent=1)



