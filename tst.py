
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

  if 1:
    k = b"test" 
    v = b"t" * 100000
    await rc.set(k,v)
    print(await rc.get(k))

  #print(  await rc.get(b"test") )
  #print(  await rc.get(b"test") )

  #//rc.stat()
  #//print(await rc.get(b'test1999900'))

  await rc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
  print("DONE")


