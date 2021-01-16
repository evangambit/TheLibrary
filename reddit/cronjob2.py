import argparse, code, json, os, random, requests, sqlite3, time
from datetime import datetime

from reddit import Reddit, create_submission, Submission
import praw

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

def merge_comment(old, new):
  print(new['author'])
  if new['author'] == '[deleted]':
    return old
  else:
    return new

if __name__ == '__main__':
  print('========' * 4)
  startTime = time.time()
  print(f'Starting cronjob2.py at {round(time.time())}s ({datetime.fromtimestamp(round(startTime))})')

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
    try:
      newPosts = reddit.request(
        f"https://www.reddit.com/r/{subreddit}/new.json?sort=new"
      )['data']['children']
    except:
      newPosts = reddit.request(
        f"https://www.reddit.com/r/{subreddit}/new.json?sort=new"
      )['data']['children']
    newPosts = [c['data'] for c in newPosts if c['kind'] == 't3']

    # Fetch new comments.
    try:
      newComments = reddit.request(
        f"https://www.reddit.com/r/{subreddit}/comments.json?limit={args.num}"
      )['data']['children']
    except:
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
        print(f'(1) Downloading all comments from {post["permalink"]}')
        newPosts[i] = get_submission(reddit, post['id'])


    # Create map of posts
    posts = {}
    for post in newPosts:
      posts[post['id']] = post

      # If we've never seen this post before, we need to do a best effort to
      # fetch all of its comments.
      if 'comments' not in post:
        print(f'(2) Downloading all comments from {post["permalink"]}')
        posts[post['id']] = get_submission(reddit, post['id'])

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
          print(f'(3) Downloading all comments from {post["permalink"]}')
          posts[postid] = get_submission(reddit, postid)

      post = posts[postid]

      cids = [comment['id'] for comment in post['comments']]

      if c['id'] in cids:
        idx = cids.index(c['id'])
        oldc = post['comments'][idx]
        if c['author'] == '[deleted]':
          continue
        post['comments'][idx] = c
        modifiedPosts.add(postid)
      else:
        post['comments'].append(c)
        modifiedPosts.add(postid)

    print(f"Writing out {len(modifiedPosts)} jsons")

    for postid in modifiedPosts:
      post = posts[postid]
      year = timestamp_to_year(post['created_utc'])
      fn = get_post_fn(args, year, postid)
      os.makedirs(os.path.split(fn)[0], exist_ok=True)
      with open(fn, 'w+') as f:
        json.dump(post, f, indent=1)


  # # Step 2: For all posts over 1 month old, re-grab all their comments to update
  # # scores.

  # Create refresh.json if it doesn't exist.
  rfp = os.path.join(args.outdir, 'refresh.json')
  if not os.path.exists(rfp):
    with open(rfp, 'w') as f:
      f.write('{}')

  # Read refresh.json from disk.
  with open(rfp, 'r') as f:
    refresh = json.load(f)

  # Read all posts on disk.
  allposts = []
  for year in os.listdir(args.outdir):
    if os.path.isdir(os.path.join(args.outdir, year)):
      allposts += [(year, x.split('.')[0]) for x in os.listdir(os.path.join(args.outdir, year)) if '.json' in x]

  # Add any new posts to refresh.json
  for year, postid in allposts:
    if postid in refresh:
      continue
    with open(os.path.join(args.outdir, year, postid + '.json'), 'r') as f:
      post = json.load(f)
      refresh[postid] = {
        "created_utc": post['created_utc'],
        "last_refreshed": 0.0
      }

  # Compute which posts to update.
  oldPostsToRefresh = []
  now = time.time()
  for postid in refresh:
    a = refresh[postid]
    if now - a['created_utc'] < kSecsPerDay * 14:
      continue
    if a['last_refreshed'] != 0.0:
      continue
    oldPostsToRefresh.append(postid)

  # Update at most 10 posts.
  for postid in oldPostsToRefresh[:10]:
    submission = Submission(reddit, postid, order='new')
    new = submission.json
    new['comments'] = [c.json for c in submission.comments.values()]

    year = timestamp_to_year(submission.json['created_utc'])
    with open(get_post_fn(args, year, postid), 'r') as f:
      old = json.load(f)

    # Fetch missing new comments
    missingIds = set(c['id'] for c in old['comments']).difference(set(c['id'] for c in new['comments']))
    for id_ in missingIds:
      url = 'https://www.reddit.com' + new['permalink'] + id_ + '.json'
      j = reddit.request(url, max_tries=3, headers=None)
      c = j[1]['data']['children']
      if len(c) == 0:
        print('comment "%s" was deleted' % (url))
        continue
      c = c[0]['data']
      assert c['id'] == id_
      new['comments'].append(c)

    C = {}
    for comment in old['comments']:
      C[comment['id']] = comment

    for comment in new['comments']:
      if comment['id'] not in C:
        # This should almost never happen.
        C[comment['id']] = comment
      else:
        C[comment['id']] = merge_comment(C[comment['id']], comment)

    if new['author'] == '[deleted]':
      new = old

    new['comments'] = list(C.values())

    with open(os.path.join('tmp', postid + '.json'), 'w+') as f:
      json.dump(new, f, indent=1)

    refresh[postid]['last_refreshed'] = time.time()

  with open(rfp, 'w+') as f:
    json.dump(refresh, f, indent=1)

  print('Ending cronjob2.py after %.1f seconds' % (time.time() - startTime))

