from pymemcache.client.base import Client

client = Client('localhost')
client.set('test', 'testtest')
result = client.get('test')

print(result)
