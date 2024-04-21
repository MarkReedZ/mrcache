
import redis
r = redis.Redis(host='localhost', port=6379, db=0)
#b = b't' * 256
b = b't' * 10000
r.set('test', b)
print(r.get('test'))
