
import redis
r = redis.Redis(host='localhost', port=6379, db=0)
r.set('test', 'testtest')
print(r.get('test'))
