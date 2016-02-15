import asyncio
import websockets
import json
import networkx as nx
from datetime import datetime

from . import WikipediaCrawler

# connected = set()
#
# @asyncio.coroutine
# def handler(websocket, path):
#     global connected
#     # Register.
#     connected.add(websocket)
#     try:
#         # Implement logic here.
#         await asyncio.wait([ws.send("Hello!") for ws in connected])
#         await asyncio.sleep(10)
#     finally:
#         # Unregister.
#         connected.remove(websocket)

async def hello(websocket, path):
    print("path:", path)

    payload = await websocket.recv()
    print("payload: {0}".format(payload))

    payload = json.loads(payload)

    request_type = payload['type']

    if request_type == "crawl":
        print("begin crawl")

        count = payload['count']

        time_stamp = payload['date']

        time_stamp = datetime(time_stamp['year'],
                              time_stamp['month'],
                              time_stamp['day'])

        wikicrawler = WikipediaCrawler(payload['title'], time_stamp, count)

        while True:
            for edge in wikicrawler.crawl_iter():

                print("edge: {0}".format(edge))

                await websocket.send("{0}".format(json.dumps(edge)))

#@asyncio.coroutine
async def handler(websocket, path):
    payload = await websocket.recv()
    print("payload: {0}".format(payload))

    payload = json.loads(payload)

    print("begin crawl")

    count = payload['count']

    time_stamp = payload['date']

    time_stamp = datetime(time_stamp['year'],
                          time_stamp['month'],
                          time_stamp['day'])

    wikicrawler = WikipediaCrawler(payload['title'], time_stamp, count)

    while True:
        for edge in wikicrawler.crawl_iter(seed_count=count):

            print("edge: {0}".format(edge))

            await websocket.send("{0}".format(json.dumps(edge)))

            listener_task = asyncio.ensure_future(websocket.recv())

            done, pending = await asyncio.wait(
                [listener_task],
                return_when=asyncio.FIRST_COMPLETED)

            if listener_task in done:
                payload = listener_task.result()

                payload = json.loads(payload)

                print("listener_task payload:", payload)

                await websocket.send("{0}".format(nx.find_cliques(wikicrawler.graph)))
            else:
                print("cancelling listener_task")
                listener_task.cancel()



start_server = websockets.serve(handler, '127.0.0.1', 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()