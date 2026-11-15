import asyncio
import websockets

async def echo(websocket):
    try:
        async for message in websocket:
            await websocket.send(message)
    except websockets.exceptions.ConnectionClosed:
        pass

async def main():
    async with websockets.serve(echo, "127.0.0.1", 8765):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
