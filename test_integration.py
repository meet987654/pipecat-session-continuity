import asyncio
import websockets
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IntegrationTest")

async def test_live_bot():
    session_id = "test_live_session_123"
    uri = f"ws://localhost:8000/ws?session_id={session_id}"
    
    logger.info(f"Connecting to bot (Session 1): {uri}")
    try:
        async with websockets.connect(uri) as ws:
            logger.info("Connected.")
            # Wait a bit for the pipeline to start and send initial greeting
            logger.info("Waiting for initial greeting...")
            for _ in range(5):
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    # If it's a string, try parsing as JSON
                    if isinstance(msg, str):
                        logger.info(f"Received JSON from bot: {msg[:100]}...")
                except asyncio.TimeoutError:
                    break
            
            logger.info("Sending fact: 'My order number is 48213'")
            # Pipecat generic client format for text
            await ws.send(json.dumps({"type": "text", "text": "My order number is 48213"}))
            
            # Wait for acknowledgment
            logger.info("Waiting for acknowledgment...")
            for _ in range(5):
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    if isinstance(msg, str):
                        logger.info(f"Bot replies: {msg[:100]}...")
                except asyncio.TimeoutError:
                    break
                    
            logger.info("Dropping connection to simulate failure!")
            # Hard close
    except Exception as e:
        logger.error(f"Error in Phase 1: {e}")

    # Wait a moment for the bot.py to detect disconnect and snapshot
    await asyncio.sleep(2)
    
    logger.info(f"\n--- Reconnecting to bot (Session 2): {uri} ---")
    try:
        async with websockets.connect(uri) as ws:
            logger.info("Connected again.")
            
            # We expect the bot to proactively speak because of the queued LLMMessagesFrame!
            logger.info("Listening for proactive bridge message...")
            for _ in range(5):
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    if isinstance(msg, str):
                        data = json.loads(msg)
                        if data.get("type") == "text" or data.get("type") == "tts_text":
                            logger.info(f">>> BOT SPOKE: {data.get('text')}")
                    # We ignore binary frames (audio) for this test printout
                except asyncio.TimeoutError:
                    break
                    
            logger.info("Asking for the fact: 'What is my order number?'")
            await ws.send(json.dumps({"type": "text", "text": "What is my order number?"}))
            
            logger.info("Waiting for the answer...")
            for _ in range(10):
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    if isinstance(msg, str):
                        data = json.loads(msg)
                        if data.get("type") == "text" or data.get("type") == "tts_text":
                            logger.info(f">>> BOT SPOKE: {data.get('text')}")
                except asyncio.TimeoutError:
                    break
            
            logger.info("Test complete.")
    except Exception as e:
        logger.error(f"Error in Phase 2: {e}")

if __name__ == "__main__":
    asyncio.run(test_live_bot())
