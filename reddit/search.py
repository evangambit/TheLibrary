from .utils import *

import mimetypes, re, sqlite3, sys, time
import http.server
import socketserver
from html.parser import HTMLParser
from urllib.parse import unquote, urlparse, parse_qs

import pystache
import spot

import cgi
import time

ext2type = mimetypes.types_map.copy()

def parse_user_query(text, rangeNames):
  ops = ['<=', '>=', '<', '>', '=']
  components = text.strip().lower().split(' ')

  tokens, ranges = [], []
  for component in components:
    isRange = False
    for op in ops:
      if op in component:
        parts = component.split(op)
        if len(parts) != 2:
          continue
        if parts[0] not in rangeNames:
          continue
        isRange = True
        ranges.append((parts[0], op, parts[1]))
        break
    if not isRange:
      tokens.append(component)
  return tokens, ranges


"""
TODO: if one token is very common and one is very rate, it can
take a very long time (e.g. 2 seconds!) to execute because an
overwhelming proportion of the common token's documents do not
contain the rare tokens.

A simple solution is to only use the rarest tokens during the initial
retrieval and check the common tokens later (either with the token
table or after retrieving the jsons).
"""
def query(index, user_query, max_results=100, offset=0):
  tokens, ranges = parse_user_query(user_query, ['age', 'depth', 'score'])

  kSecsPerHour = 60 * 60
  kSecsPerDay = kSecsPerHour * 24
  kSecsPerWeek = kSecsPerDay * 7
  kSecsPerYear = kSecsPerDay * 365.25

  kOppositeOperation = {
    '<': '>',
    '>': '<',
  }

  for i, r in enumerate(ranges):
    varname, op, val = r
    if len(val) == 0:
      return f'Failed to parse query "{user_query}"'
    if varname == 'age':
      if val[-1] not in ['y', 'w', 'd', 'h']:
        return 'Age limits must have a unit (e.g. "3d" for "3 days")'
      if val[-1] == 'y':
        dt = float(val[:-1]) * kSecsPerYear
      elif val[-1] == 'w':
        dt = float(val[:-1]) * kSecsPerWeek
      elif val[-1] == 'd':
        dt = float(val[:-1]) * kSecsPerDay
      elif val[-1] == 'h':
        dt = float(val[:-1]) * kSecsPerHour
      t = float(time.time() - dt)
      ranges[i] = ('created_utc', kOppositeOperation[op], t)

  if len(tokens) == 0:
    it = index.all_iterator(
      ranking='-score',
      limit=float('inf'),
      range_requirements=ranges,
      offset=offset
    )
  else:
    it = spot.filtering.intersect(
      *[index.token_iterator(
        t,
        ranking='-score',
        limit=float('inf'),
        range_requirements=ranges,
        offset=offset
      ) for t in tokens],
      limit=max_results
    )

  R = []
  try:
    for i in range(max_results):
      R.append(next(it))
  except StopIteration:
    pass

  R = [
    index.json_from_docid(r[1]) for r in R
  ]
  for i in range(len(R)):
    T = R[i]["tokens"].split(' ')
    T.sort()
    R[i]["tokens"] = ' '.join(T)
  return {
    "comments": R,
    "tokens": tokens
  }

class FindAndBoldTermsHTMLParser(HTMLParser):
  def __init__(self):
    super().__init__()
    self.terms = set()

  def reset(self):
    super().reset()
    self.html = ''

  def handle_starttag(self, tag, attrs):
    r = '<' + tag
    for key, value in attrs:
      r += f' {key}="{value}"'
    r += '>'
    self.html += r

  def handle_endtag(self, tag):
    self.html += f'</{tag}>'

  def handle_data(self, data):
    for term in self.terms:
      data = re.sub(
        re.compile(f"[^\\w\\d]({term})[^\\w\\d]", re.IGNORECASE),
        r" <span class='term'>\1</span> ",
        data,
      )
      data = re.sub(
        re.compile(f"^({term})[^\\w\\d]", re.IGNORECASE),
        r"<span class='term'>\1</span> ",
        data,
      )
    # self.html += cgi.escape(data)
    self.html += data

boulder = FindAndBoldTermsHTMLParser()


def search(query_text, max_results=100):
  if len(query_text.strip()) == 0:
    with open('reddit/template.html', 'r') as f:
      text = f.read()
    result = pystache.render(text, {
      'comments': [],
      'num_results_msg': ''
    })
    return result

  index = spot.Index('reddit/spot-index')
  start_time = time.time()
  print(f'search {query_text}')


  query_result = query(index, query_text, max_results = max_results+1)
  if type(query_result) is str:
    return "Error 500"

  for comment in query_result['comments']:
    tokens = comment["tokens"].split(' ')
    tokens = [t for t in tokens if t[:8] == 'pauthor:']
    if len(tokens) > 0:
      comment["pauthor"] = tokens[0][8:]

  tokens = query_result['tokens']

  parser = MyHTMLParser()

  boulder.terms = set([t for t in tokens if (':' not in t) and (t not in '()+')])
  for i in range(len(query_result['comments'])):
    comment = query_result['comments'][i]
    comment['subreddit'] = 'slatestarcodex' if 'slatestarcodex' in comment['permalink'] else 'TheMotte'
    comment['idx'] = i + 1
    parser.reset()
    if "body_html" in comment:
      parser.feed(comment["body_html"])
    else:
      header = f'<h2>{comment.get("title", "")}</h2>'
      body = comment.get('selftext_html', '')
      if body is None:
        body = ''
      parser.feed(header + body) 
    boulder.reset()
    boulder.feed(parser.alltext)
    comment['body_html'] = boulder.html

  with open('reddit/template.html', 'r') as f:
    text = f.read()
  dt = time.time() - start_time
  msg = f'Over {max_results} results in %.3f seconds' % dt if len(query_result['comments']) == max_results + 1 else f'{len(query_result["comments"])} results in %.3f seconds' % dt
  result = pystache.render(text, {
    'comments': query_result['comments'],
    'num_results_msg': msg
  })
  return result

