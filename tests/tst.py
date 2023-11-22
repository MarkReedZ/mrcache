
import asyncio, time
import asyncmrcache

import tracemalloc
tracemalloc.start()

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def lcb(client):
  print("Lost connection")

async def run(loop):

  rc = await asyncmrcache.create_client( "localhost", loop, lost_cb=lcb)

  v = b"value" * 20
  if 0:
    for x in range(200000):
      k = bytes("test" + str(x), "utf-8")
      await rc.set(k,v)

  act = await rc.get(b"test199998")
  if v != act:
    print( "Act >" + str(act) + "< != >" + str(v) + "<" )

  await rc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
  print("DONE")


