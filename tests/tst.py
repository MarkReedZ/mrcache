
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

  await rc.set(b"test", b"a"*512)
  await rc.getz(b"test")
  exit()

  num = 99000
  v = b"v" * 1000
  l = []
  l.append( b"0" * 1000 )
  l.append( b"1" * 1000 )
  l.append( b"2" * 1000 )
  l.append( b"3" * 1000 )
  l.append( b"4" * 1000 )
  l.append( b"5" * 1000 )
  l.append( b"6" * 1000 )
  l.append( b"7" * 1000 )
  for x in range(num):
    k = bytes("test" + str(x), "utf-8")
    await rc.set(k,l[x%8])

  hit = 0
  for x in range(num):
    k = bytes("test" + str(x), "utf-8")
    v = await rc.get(k)
    if v != None:
      if v != l[x%8]:
        print( "ERROR", k)
      hit += 1

  print("Hit =",hit)
  rc.stat()

  #act = await rc.get(b"test2")
  #if v != act:
    #print( "Act >" + str(act) + "< != >" + str(v) + "<" )

  await rc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
  print("DONE")


