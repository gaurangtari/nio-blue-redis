import asyncio
import aiohttp
import logging
from aiohttp import web
from aiohttp.web_ws import WSMsgType

logging.basicConfig(level=logging.INFO)

clients = []

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    logging.info('Client connected')
    clients.append(ws)

    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            logging.info(f"Received message: {msg.data}")
            for client in clients:
                if client is not ws:
                    await client.send_str(msg.data)
        elif msg.type == WSMsgType.ERROR:
            logging.error(f"WebSocket connection closed with exception {ws.exception()}")

    logging.info('Client disconnected')
    clients.remove(ws)
    return ws

async def init_app():
    app = web.Application()
    app.router.add_route('*', '/ws', websocket_handler)
    return app

if __name__ == '__main__':
    app = init_app()
    web.run_app(app, port=8080)
