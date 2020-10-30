from django.http import HttpResponse

from . import search

def index(request):
  query_text = request.GET.get('query', '')
  t = search.search(query_text)
  return HttpResponse(t)

