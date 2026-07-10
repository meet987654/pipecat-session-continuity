import asyncio
import os
import sys
import uuid
import logging
from fastapi import FastAPI, WebSocket, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineTask
from pipecat.pipeline.runner import PipelineRunner
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams,
)

from pipecat_session_continuity import SessionContinuity
from pipecat_session_continuity.storage.sqlite_storage import SQLiteStorage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Serve the static HTML frontend
app.mount("/client", StaticFiles(directory="examples/voice-assistant/client"), name="client")

running_tasks = {}

# Use SQLite for the demo so it runs anywhere without Docker
storage = SQLiteStorage(db_path="examples/voice-assistant/sessions.db")
session_continuity = SessionContinuity(storage_backend=storage, stale_threshold_minutes=30)

@app.post("/create_session")
async def create_session():
    # In production, secure this. For demo, just generate an ID.
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    return JSONResponse({"session_id": session_id, "signature": "mock-signature"})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, session_id: str, signature: str = None, reconnect: bool = False):
    await websocket.accept()

    # Load environment variables (Make sure you have OPENAI_API_KEY, CARTESIA_API_KEY, DEEPGRAM_API_KEY in .env)
    from dotenv import load_dotenv
    load_dotenv()

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_out_enabled=True,
            add_wav_header=True,
            vad_enabled=True,
            vad_audio_passthrough=True,
            vad_analyzer_type="webrtc",
        )
    )

    llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"), model="gpt-4o-mini")
    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
    tts = CartesiaTTSService(api_key=os.getenv("CARTESIA_API_KEY"), voice_id="a0e99841-438c-4a64-b679-ae501e7d6091")

    context = LLMContext(
        messages=[{"role": "system", "content": "You are a helpful voice assistant. Keep answers brief (1-2 sentences) and natural."}]
    )
    context_aggregator = LLMContextAggregatorPair(context=context)

    pipeline = Pipeline([
        transport.input(),
        stt,
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant(),
    ])

    task = PipelineTask(pipeline)
    
    turn_observer = task.turn_tracking_observer
    if turn_observer:
        @turn_observer.event_handler("on_turn_ended")
        async def on_turn_ended(observer, *args, **kwargs):
            await session_continuity.checkpoint(context, session_id, {})
            logger.info(f"Checkpointed context for {session_id} (Length: {len(context.get_messages())})")

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info(f"Client connected for session {session_id}")
        is_resumed, pending = await session_continuity.resume_or_start(task, context, session_id)
        if is_resumed:
            logger.info(f"Successfully resumed session {session_id}")

    runner = PipelineRunner()
    running_tasks[session_id] = asyncio.create_task(runner.run(task))
    
    # Wait until connection is dropped
    try:
        while True:
            await asyncio.sleep(1)
            if not running_tasks[session_id] or running_tasks[session_id].done():
                break
    except asyncio.CancelledError:
        pass
    finally:
        logger.info(f"WebSocket closed for {session_id}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
