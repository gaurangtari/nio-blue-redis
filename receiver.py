import argparse
import asyncio
import logging
import aiohttp
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate

logging.basicConfig(level=logging.INFO)

async def run(pc, signaling_url):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(signaling_url) as ws:
            async for msg in ws:
                data = json.loads(msg.data)
                if data["type"] == "offer":
                    await pc.setRemoteDescription(RTCSessionDescription(data["sdp"], "offer"))
                    answer = await pc.createAnswer()
                    await pc.setLocalDescription(answer)
                    await ws.send_str(json.dumps({"type": "answer", "sdp": answer.sdp}))
                elif data["type"] == "answer":
                    await pc.setRemoteDescription(RTCSessionDescription(data["sdp"], "answer"))
                elif data["type"] == "candidate":
                    await pc.addIceCandidate(RTCIceCandidate(data["candidate"]))

async def main():
    parser = argparse.ArgumentParser(description="Receive RTP video over WebRTC")
    parser.add_argument("--signaling-url", type=str, default="ws://localhost:8080/ws")
    args = parser.parse_args()

    signaling_url = args.signaling_url
    pc = RTCPeerConnection()

    try:
        await run(pc=pc, signaling_url=signaling_url)
    except KeyboardInterrupt:
        pass
    finally:
        await pc.close()

if __name__ == "__main__":
    asyncio.run(main())
