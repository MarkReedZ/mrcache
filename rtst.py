
from redis import Redis
redis = Redis(host="localhost", port=6379, db=0)
redis.set("test","test"*25*100)
#redis.set("test","test"*4)
print(redis.get("test"))
redis.close()
