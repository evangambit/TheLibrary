from utils import *

import spot

import time

if os.path.exists('spot-index'):
  os.remove('spot-index')

index = spot.Index.create('spot-index', rankings=['score'], ranges=['created_utc', 'score', 'depth', 'random'])

comment_insertions = 0
token_insertions = 0

lasttime = time.time()

ids = set()
allscores = []
for thread in threads():
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

    depth = 0
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

    tokens = get_tokens(comment, parent, gparent, thread, isthread=False)
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
    index.insert(id_, tokens, comment)
    comment_insertions += 1
    token_insertions += len(tokens)

    if comment_insertions % 10000 == 0:
    	print('%.3f' % (time.time() - lasttime), comment_insertions, token_insertions)
    	lasttime = time.time()

index.create_indices()

index.commit()

print(comment_insertions, 'comments inserted')
print(token_insertions, 'tokens inserted')
