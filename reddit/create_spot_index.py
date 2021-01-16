from utils import *

import spot

import time

if os.path.exists('reddit/spot-index'):
  os.remove('reddit/spot-index')

index = spot.Index.create('reddit/spot-index', rankings=['score', 'created_utc'], ranges=['created_utc', 'score', 'depth', 'random'])

comment_insertions = 0
token_insertions = 0

lasttime = time.time()

ids = set()
allscores = []
# for thread in threads(years=['2019', '2020']):
for thread in threads():
  comments = thread['comments']

  id2comment = {}
  for comment in comments:
    id2comment[comment['id']] = comment

  for comment in comments:
    if 'body_html' not in comment:
      continue
    if comment.get('body', '') == '[deleted]':
      continue
    if comment['body_html'] == '<div class="md"><p>[deleted]</p>\n</div>':
      continue

    # Threads have depth = 0
    # All comments have depth > 0
    depth = 1
    parent = id2comment.get(comment['parent_id'][3:], None)
    if parent:
      depth += 1
      ancestor = id2comment.get(parent['parent_id'][3:], None)
      while ancestor and (ancestor['parent_id'][3:] in id2comment):
        depth += 1
        ancestor = id2comment.get(ancestor['parent_id'][3:], None)

    comment['depth'] = depth

    tokens = get_tokens(comment, parent, thread, isthread=False)
    comment['tokens'] = ' '.join(tokens)

    postid = permalink2postid(comment['permalink'])

    # Save some space -- all this information is in body_html anyway
    if 'body' in comment:
      del comment['body']

    if 'score' not in comment:
      comment['score'] = comment.get('ups', 0)

    id_ = int(comment['id'], 36)
    if id_ in ids:
      continue
    ids.add(id_)

    comment['random'] = random.random()
    index.insert(id_, postid, comment['created_utc'], tokens, comment)
    comment_insertions += 1
    token_insertions += len(tokens)

    if comment_insertions % 10000 == 0:
    	print('%.3f' % (time.time() - lasttime), comment_insertions, token_insertions)
    	lasttime = time.time()

  continue
  del thread['comments']
  postid = int(thread['id'], 36)
  tokens = get_tokens(thread, parent, thread, isthread=True)
  thread['depth'] = 0
  thread['tokens'] = ' '.join(tokens)
  thread['random'] = random.random()
  thread['score'] = thread.get('ups', 0)
  # There's some bug that sometimes duplicates a thread across two different
  # year directories (afaict this is only for threads between 2019 and 2020).
  # Since 'threads' runs chronologically (oldest to newest) we simply replace
  # the thread from 2019 (which is empirically always less up-to-date) with the
  # thread from 2020.
  index.replace(postid, postid, thread['created_utc'], tokens, thread)

index.create_indices()

index.commit()

print(comment_insertions, 'comments inserted')
print(token_insertions, 'tokens inserted')
