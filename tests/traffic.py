
import asyncio, time, random
import asyncmrcache

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def lcb(client):
  print("Lost connection")

async def run(loop):

  rc = await asyncmrcache.create_client( [("localhost",7000)], loop, lost_cb=lcb)

  num = 10000

  cac = {}

  for x in range(num):
    k = random.randbytes( random.randrange(1, 2048) )
    v = random.randbytes( random.randrange(1, 8096) )
    cac[k] = v
    await rc.set(k,v)

  s = time.time()
  hit = 0
  #while (time.time() - s) < 5:
  #for x in range(100000):
  while 1:
    k,exp = random.choice(list(cac.items()))
    if random.getrandbits(2) == 0:
      await rc.set(k,exp)
    else:
      v = await rc.get(k)
      if v != exp:
        print( "ERROR", k)
        print( "Saw: ", v)
        print( "exp: ", exp)
        print( "Key len: ", len(k))
        print( "Val len: ", len(exp))

  rc.stat()

  #act = await rc.get(b"test2")
  #if v != act:
    #print( "Act >" + str(act) + "< != >" + str(v) + "<" )

  await rc.close()

if __name__ == '__main__':
  s = time.time()
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
  print( time.time - s, " seconds")
  print("DONE")


