import argparse, json, os
from datetime import datetime

pjoin = os.path.join

from reddit import Reddit, Submission, create_submission

kSecsPerDay = 60*60*24 # 86_400

def refresh(reddit, subreddits, days, outdir):
  if not os.path.exists(outdir):
    os.mkdir(outdir)

  # r/TheMotte is typically very slow per comment on account of its habit
  # of having threads with over 400 comments.
  for subreddit in subreddits:
    # Grab all posts within the last time period
    S = reddit.new_submissions(subreddit, max_age=kSecsPerDay*days)

    # Grab all comments for these posts
    for s in S:
      fn = s['id'] + '.json'
      year = str(datetime.utcfromtimestamp(s['created_utc']).year)
      print('https://www.reddit.com' + s['permalink'])

      submission = create_submission(reddit, s['id'])

      if not os.path.exists(pjoin(outdir, year)):
        os.mkdir(pjoin(outdir, year))

      if os.path.exists(pjoin(outdir, year, fn)):
        with open(pjoin(outdir, year, fn), 'r') as f:
          old = json.load(f)
      else:
        old = {'comments': []}

      # Copy over old comments.
      for c in old['comments']:
        if c['id'] not in submission.comments:
          submission.comments[c['id']] = c

      print(len(submission.comments), 'out of', submission.json['num_comments'])

      j = submission.json
      j['comments'] = list(submission.comments.values())

      with open(pjoin(outdir, year, fn), 'w+') as f:
        json.dump(j, f)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Recursively grab comments from every post in the last few days')
  parser.add_argument('--days', '-d', type=float, required=True, help='Number of days')
  parser.add_argument('--outdir', '-o', type=str, required=True, help='Directory to dump jsons to')
  parser.add_argument('--subs', '-s', type=str, required=False, default='TheMotte,slatestarcodex,theschism', help='Comma-delimited list of subreddits')
  args = parser.parse_args()

  reddit = Reddit()

  subreddits = args.subs.split(',')

  refresh(reddit, subreddits, args.days, args.outdir)

