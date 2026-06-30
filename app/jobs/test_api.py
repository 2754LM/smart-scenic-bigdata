import urllib.request
r = urllib.request.urlopen('http://localhost:8000/api/realtime/kafka/status')
print(r.status, r.read()[:200])