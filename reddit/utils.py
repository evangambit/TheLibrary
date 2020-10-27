import argparse, array, hashlib, json, os, random, re, shutil, sqlite3
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urlparse
pjoin = os.path.join

def pad(t, n, c=' '):
  return max(n - len(t), 0) * c + t

class Hash64:
  def __init__(self):
    pass
  def __call__(self, x):
    h = hashlib.sha256()
    h.update(x.encode())
    return int(h.hexdigest()[-16:], 16) - (1<<63)
hashfn = Hash64()

kSecsPerDay = 60 * 60 * 24  # 86,400
kSecsPerWeek = kSecsPerDay * 7

kTokenBlacklist = set(["a", "the", "to", "of", "and", "that", "is"])

kUrlRegex = r"https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)"

class MyHTMLParser(HTMLParser):
  def __init__(self, ignore_quotes=True):
    super().__init__()
    self.ignore_quotes = ignore_quotes
  def reset(self):
    super().reset()
    self.blockquote = 0
    self.text = ''
    self.alltext = '' # Includes quoted text.
    self.links = set()
  def handle_starttag(self, tag, attrs):
    if tag == 'blockquote':
      self.alltext += '<quote>'
      self.blockquote += 1
    elif tag == 'a':
      self.alltext += '<a>'
      href = [x for x in attrs if x[0] == 'href']
      self.links.add(href[0][1])
  def handle_endtag(self, tag):
    if tag == 'blockquote':
      self.alltext += '</quote>'
      self.blockquote -= 1
    elif tag == 'a':
      self.alltext += '</a>'
  def handle_data(self, data):
    isUrl = bool(re.match(kUrlRegex, data))
    self.alltext += data
    if (self.blockquote == 0 or self.ignore_quotes) and not isUrl:
      self.text += data

def threads(years=None):
  base = 'comments'
  if years is None:
    years = os.listdir(base)
  for year in years:
    if not year.isdigit():
      continue
    for fn in os.listdir(pjoin(base, year)):
      if fn[-5:] != '.json':
        continue
      path = pjoin(base, year, fn)
      with open(pjoin(base, year, fn), 'r') as f:
        J = json.load(f)
      yield J

parser = MyHTMLParser()

def getscore(comment):
  return comment.get('score', 0)

"""
TODO: "i.e." should become "i.e."
"""
def text2tokens(text):
  text = text.lower()
  text = re.sub(r"[^\w\d%@#$^&']+", " ", text)
  text = ' ' + text + ' '
  text = text.replace(" '", " ")
  text = text.replace("' ", " ")
  tokens = set(text.strip().split(' '))
  if '' in tokens:
    tokens.remove('')
  for token in kTokenBlacklist:
    if token in tokens:
      tokens.remove(token)
  return tokens

def get_tokens(comment, parent, gparent, thread, isthread):
  parser.reset()
  parser.feed(comment['body_html'])
  parser.close()

  links = parser.links
  alltext = parser.alltext
  if thread:
    iscw = ('culture_war_roundup' in thread['url'])

  # Add comment's words to tokens.
  tokens = text2tokens(parser.text)
  assert '"' not in ' '.join(tokens)

  # Add author to tokens
  if 'author' in comment:
    tokens.add(f'author:{comment["author"].lower()}')

  date = datetime.fromtimestamp(comment['created_utc'])
  tokens.add(f'year:{date.year}')
  tokens.add(f'month:{date.month}')

  # Add CW indicator
  if thread:
    if 'culture_war_roundup' in thread['url']:
      tokens.add('misc:cw' if iscw else 'misc:notcw')

  if not isthread:
    tokens.add(f'depth:{comment["depth"]}')

  tokens.add(f'score:{getscore(comment)}')
  if thread:
    s = thread["subreddit"]
    if s[:2] == 'r/':
      s = s[2:]
    if s == 't5_30m6u':
      s = 'slatestarcodex'
    elif s == 't5_vkedk':
      s = 'TheMotte'
    tokens.add(f'sub:{s.lower()}')

  domains = set()
  for link in parser.links:
    loc = urlparse(link).netloc
    if loc[:4] == 'www.':
      loc = loc[4:]
    elif loc[:3] == 'en.':
      loc = loc[3:]
    domains.add(loc)
  for domain in domains:
    tokens.add(f'linksto:{domain}')
  
  if parent:
    if 'author' in parent:
      tokens.add(f'pauthor:{parent["author"].lower()}')
    tokens.add(f'pscore:{getscore(parent)}')

  if gparent:
    if 'author' in gparent:
      tokens.add(f'gauthor:{gparent["author"].lower()}')
    tokens.add(f'gscore:{getscore(gparent)}')

  if 'distinguished' in comment and comment['distinguished'] is not None:
    if comment['distinguished'] != 'moderator':
      print(comment['distinguished'], type(comment['distinguished']))
    tokens.add('misc:modhat')

  return tokens

assert text2tokens("'I've done things – foo bar!'") == set([
  "i've", "done", "foo", "bar", "things"
])

assert text2tokens("you're bar (and doesn't endorse).") == set([
  'you\'re', 'bar', 'doesn\'t', 'endorse'
])

assert text2tokens('When people say "Red tribe" "incorrectly", it\'s usually') == set([
  'tribe', 'say', 'usually', 'red', 'incorrectly', 'when', "it's", 'people'
])

assert text2tokens("because they're not particularly concerned with the distinction between Republican and Red. They're trying") == set([
  'concerned', 'distinction', 'because', 'between', 'trying', 'with', "they're", 'red', 'not', 'particularly', 'republican'
])

