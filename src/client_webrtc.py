import asyncio
import httpx
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration
from aiortc.contrib.signaling import object_to_string
import json
from loguru import logger
import sys

# Setup logger
logger.remove()
logger.add(sys.stdout, format="{time} | {level} | {message}", level="DEBUG")

async def run(session_id=None):
    pc = RTCPeerConnection()

    channel = pc.createDataChannel("pipecat")

    @channel.on("open")
    def on_open():
        logger.info("Data channel open!")
        
        # We simulate the user saying a message by sending an app message that the bridge might intercept
        # but wait, session_continuity listens for context updates. To trigger a context update,
        # we can just send an app message that we handle, or better, the bridge intercepts CheckpointFrames.
        # Actually, in bot.py, we just want to verify state is restored.
        pass

    @channel.on("message")
    def on_message(message):
        logger.info(f"Received message via data channel: {message}")

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        logger.info(f"Connection state is {pc.connectionState}")
        if pc.connectionState == "failed":
            await pc.close()

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    # Send offer to server
    async with httpx.AsyncClient() as client:
        payload = {
            "sdp": pc.localDescription.sdp,
            "type": pc.localDescription.type,
        }
        if session_id:
            payload["session_id"] = session_id
            
        resp = await client.post("http://127.0.0.1:8001/connect", json=payload)
        resp_data = resp.json()
        
    session_id = resp_data["session_id"]
    logger.info(f"Got session id: {session_id}")

    answer = RTCSessionDescription(sdp=resp_data["sdp"], type=resp_data["type"])
    await pc.setRemoteDescription(answer)

    # Wait a bit
    await asyncio.sleep(5)
    
    # Check redis for saved context
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    keys = r.keys(f"pipecat:session:{session_id}")
    logger.info(f"Redis keys for session {session_id}: {keys}")
    
    # Keep alive for a bit
    await asyncio.sleep(5)
    await pc.close()
    return session_id

if __name__ == "__main__":
    session_id = None
    if len(sys.argv) > 1:
        session_id = sys.argv[1]
    asyncio.run(run(session_id))
