
import asyncio, time, random
import asyncmrcache

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def lcb(client):
  print("Lost connection")

async def run(loop):

  rc = await asyncmrcache.create_client( [("localhost",7000)], loop, lost_cb=lcb)

  rc.stat()

  await rc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()


