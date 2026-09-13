import asyncio
import json
import os
import uuid
from aiohttp import web, WSMsgType

# clients: { client_id: {"ws": websocket, "name": "Alice", "status": "available"} }
clients = {}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(BASE_DIR, "index.html")


def get_member_list():
    return [
        {"id": cid, "name": info["name"], "status": info["status"]}
        for cid, info in clients.items()
    ]


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
            msg_type = data.get("type")

            if msg_type == "join":
                clients[client_id] = {
                    "ws": ws,
                    "name": data.get("name", "Unknown"),
                    "status": "available",
                }
                print(f"{clients[client_id]['name']} joined. Total: {len(clients)}")
                await ws.send_str(json.dumps({"type": "your_id", "id": client_id}))
                await broadcast_member_list()
                continue

            if msg_type == "status":
                # Client telling us they're now in a call, or free again
                if client_id in clients:
                    clients[client_id]["status"] = data.get("status", "available")
                    await broadcast_member_list()
                continue

            # offer / answer / candidate / hangup all get routed straight
            # to the specific person they're meant for
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
    port = int(os.environ.get("PORT", 8000))
    print(f"Combined server running on http://0.0.0.0:{port}")
    print("WebSocket endpoint at /ws")
    web.run_app(app, host="0.0.0.0", port=port)