import asyncio
import json
import os
import uuid
from aiohttp import web, WSMsgType

# clients: { client_id: {"ws": websocket, "name": "Alice"} }
clients = {}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(BASE_DIR, "index.html")


def get_member_list():
    # Sends everyone's id + name to all clients, so they can render the list
    return [{"id": cid, "name": info["name"]} for cid, info in clients.items()]


async def broadcast_member_list():
    message = json.dumps({"type": "member_list", "members": get_member_list()})
    for info in clients.values():
        if not info["ws"].closed:
            await info["ws"].send_str(message)


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    client_id = str(uuid.uuid4())[:8]

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue

            data = json.loads(msg.data)

            if data.get("type") == "join":
                # First message a client sends: their chosen name
                clients[client_id] = {"ws": ws, "name": data.get("name", "Unknown")}
                print(f"{clients[client_id]['name']} joined. Total: {len(clients)}")
                # Tell this client their own assigned id, so the page can
                # tell itself apart from everyone else in the member list
                await ws.send_str(json.dumps({"type": "your_id", "id": client_id}))
                await broadcast_member_list()
                continue

            # Every other message (offer/answer/candidate) should be routed
            # ONLY to the specific person it's meant for, using "to"
            target_id = data.get("to")
            if target_id and target_id in clients:
                data["from"] = client_id
                await clients[target_id]["ws"].send_str(json.dumps(data))

    finally:
        if client_id in clients:
            print(f"{clients[client_id]['name']} disconnected.")
            del clients[client_id]
            await broadcast_member_list()

    return ws


async def index_handler(request):
    return web.FileResponse(INDEX_PATH)


app = web.Application()
app.router.add_get('/', index_handler)
app.router.add_get('/ws', websocket_handler)

if __name__ == "__main__":
    print("Combined server running on http://0.0.0.0:8000")
    print("WebSocket endpoint at /ws")
    web.run_app(app, host="0.0.0.0", port=8000)