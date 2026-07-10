import asyncio
import os
import sys
import uuid
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from loguru import logger

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineTask
from pipecat.pipeline.runner import PipelineRunner
from pipecat.services.groq.llm import GroqLLMService
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.base_transport import TransportParams

from pipecat_session_continuity import SessionContinuity

# Setup logger
logger.remove()
logger.add(sys.stdout, format="{time} | {level} | {message}", level="DEBUG")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global dict to hold running tasks
running_tasks = {}

@app.post("/connect")
async def connect(request: Request):
    data = await request.json()
    session_id = data.get("session_id")
    offer_sdp = data.get("sdp")
    offer_type = data.get("type")
    
    if not session_id:
        session_id = f"sess_{uuid.uuid4().hex[:8]}"

    logger.info(f"Connecting WebRTC for session: {session_id}")
    
    # Initialize SmallWebRTCConnection
    webrtc_connection = SmallWebRTCConnection()
    
    # Setup answer
    await webrtc_connection.initialize(offer_sdp, offer_type)
    answer = webrtc_connection.get_answer()
    
    # Setup transport
    transport = SmallWebRTCTransport(
        webrtc_connection,
        TransportParams(
            audio_out_enabled=False,
            video_out_enabled=False,
            audio_in_enabled=False,
            video_in_enabled=False
        )
    )

    # Load environment variables if not loaded
    from dotenv import load_dotenv
    load_dotenv()
    
    llm = GroqLLMService(
        api_key=os.getenv("GROQ_API_KEY", "mock-key"),
        model="llama-3.3-70b-versatile"
    )

    context = LLMContext(
        messages=[{"role": "system", "content": "You are a helpful assistant. Reply concisely."}]
    )
    from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
    context_aggregator = LLMContextAggregatorPair(context=context)

    # Bridge
    session_continuity = SessionContinuity()

    pipeline = Pipeline([
        transport.input(),
        context_aggregator.user(),
        llm,
        context_aggregator.assistant(),
        transport.output(),
    ])

    from pipecat.pipeline.worker import PipelineParams
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,
            enable_metrics=False,
            enable_usage_metrics=False,
            enable_turn_tracking=True,
        ),
    )
    
    turn_observer = task.turn_tracking_observer
    if turn_observer:
        @turn_observer.event_handler("on_turn_ended")
        async def on_turn_ended(observer, *args, **kwargs):
            logger.info("Turn ended, saving checkpoint!")
            await session_continuity.checkpoint(context, session_id, {})
    
    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        is_resumed, pending = await session_continuity.resume_or_start(task, context, session_id)
        if not is_resumed:
            context.add_message({"role": "user", "content": "Hello! My name is WebRTC Tester!"})
            context.add_message({"role": "assistant", "content": "Nice to meet you, WebRTC Tester!"})
        
        logger.info(f"Client connected! Is resume: {is_resumed}. Current messages: {context.messages}")
        
        # Manually checkpoint to verify session continuity
        await session_continuity.checkpoint(context, session_id, {})
        logger.info(f"Manually checkpointed session {session_id}")

    runner = PipelineRunner()
    running_tasks[session_id] = asyncio.create_task(runner.run(task))

    return JSONResponse({
        "sdp": answer["sdp"],
        "type": answer["type"],
        "session_id": session_id
    })

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
