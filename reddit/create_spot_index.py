from utils import *

import spot

import time

if os.path.exists('reddit/spot-index'):
  os.remove('reddit/spot-index')

index = spot.Index.create('reddit/spot-index', rankings=['score'], ranges=['created_utc', 'score', 'depth', 'random'])

comment_insertions = 0
token_insertions = 0

lasttime = time.time()

ids = set()
allscores = []
for thread in threads(years=['2020']):
  comments = thread['comments']

  id2comment = {}
  for comment in comments:
    id2comment[comment['id']] = comment

  for comment in comments:
    if 'body_html' not in comment:
      continue
    if comment['body'] == '[deleted]':
      continue
    if comment['body_html'] == '<div class="md"><p>[deleted]</p>\n</div>':
      continue

    # Threads have depth = 0
    # All comments have depth > 0
    depth = 1
    parent = id2comment.get(comment['parent_id'][3:], None)
    if parent:
      depth += 1
      gparent = id2comment.get(parent['parent_id'][3:], None)
      ancestor = gparent
      while ancestor and (ancestor['parent_id'][3:] in id2comment):
        depth += 1
        ancestor = id2comment.get(ancestor['parent_id'][3:], None)
    else:
      gparent = None

    comment['depth'] = depth

    tokens = get_tokens(comment, parent, thread, isthread=False)
    comment['tokens'] = ' '.join(tokens)

    # Save some space -- all this information is in body_html anyway
    del comment['body']

    if 'score' not in comment:
      comment['score'] = comment.get('ups', 0)

    id_ = int(comment['id'], 36)
    if id_ in ids:
      continue
    ids.add(id_)

    comment['random'] = random.random()
    index.insert(id_, comment['created_utc'], tokens, comment)
    comment_insertions += 1
    token_insertions += len(tokens)

    if comment_insertions % 10000 == 0:
    	print('%.3f' % (time.time() - lasttime), comment_insertions, token_insertions)
    	lasttime = time.time()

  tokens = get_tokens(thread, parent, thread, isthread=True)
  del thread['comments']
  thread['depth'] = 0
  thread['tokens'] = ' '.join(tokens)
  thread['random'] = random.random()
  index.insert(int(thread['id'], 36), thread['created_utc'], tokens, thread)


index.create_indices()

index.commit()

print(comment_insertions, 'comments inserted')
print(token_insertions, 'tokens inserted')
