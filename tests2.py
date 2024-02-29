
import asyncio, time, random
import asyncmrcache

import tracemalloc
tracemalloc.start()

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def lcb(client):
  print("Lost connection")

async def run(loop):

  rc = await asyncmrcache.create_client( [("localhost",7000)], loop, lost_cb=lcb)

  #for uid in karma.keys():
    #futures.append( updateUserKarma(uid, karma[uid]) )
    #if len(futures) > 100:
      #done, pending = await asyncio.wait( futures, timeout=1 )
      #futures = list(pending)
#
  #if len(futures) > 0:
    #done, pending = await asyncio.wait( futures, timeout=2 )

  if 1:
    keys = []
    for x in range(5000000):
      k = '%030x' % random.randrange(16**30)
      k = k[0:random.randrange(29)+1]
      k = k.encode()
      keys.append(k)
    l = []
    n = 1
    while 1:
     
      k = random.choice(keys) 
      await rc.set(k,k)
      l.append(k)
      if len(l) > 1000000:
        l = l[-1000:]
      k = random.choice(l)
      act = await rc.get(k)
      if k != act:
        print("  ERR k=",k)
        exit(1)
      n += 1
      if n % 100000 == 0:
        print(n)
      if n % 1000000 == 0:
        rc.stat()

      
  if 0:
    exp = {}
    st = time.time()
    for x in range(1300000):
      k = '%030x' % random.randrange(16**30)
      k = k[0:random.randrange(29)+1]
      k = k.encode()
      v = k
      #k = ("test" + str(x)).encode()
      #v = (("test" * 5) + str(x)).encode()
      #k = b"t"
      #v = b"t"
      exp[k] = v
      await rc.set(k,v)
    print(" set took ", time.time() - st )
      
    st = time.time()
    futs = []
    i = 0
    miss = 0
    for k in exp.keys():
      act = await rc.get(k)
      if act == None:
        print("DELME miss? k=",k)
        miss += 1
      elif act != exp[k]:
        print("DELME act != exp k=",k)

  
    #futs.append( rc.get(k) )
    #if len(futs) > 100:
      #results = await asyncio.gather(*futs, return_exceptions=True)
      #for res in results:
        #print( i, ": ", res)
        #i += 1
      #futs = []
      #done, pending = await asyncio.wait( futs, timeout=1 )
      #futs = list(pending)
  #if len(futs) > 0:
    #done, pending = await asyncio.wait( futs, timeout=1 )
  #results = await asyncio.gather(*futs, return_exceptions=True)
  #for res in results:
    #print( i, ": ", res)
    #i += 1
  print(" gets took ", time.time() - st )

  print(" Saw ",miss," misses out of ", len(exp))
  rc.stat()
  print(await rc.get(b'test233'))

  await rc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
  print("DONE")


