
import asyncio, time
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


  exp = {}
  st = time.time()
  for x in range(1000000):
    k = ("test" + str(x)).encode()
    v = (("test" * 5) + str(x)).encode()
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


