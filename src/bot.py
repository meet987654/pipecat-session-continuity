import asyncio
import os
import sys
from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
import uvicorn
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipecat_session_continuity import SessionContinuity

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.services.groq.llm import GroqLLMService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.deepgram.tts import DeepgramTTSService
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams,
)
from pipecat.frames.frames import EndFrame, LLMMessagesAppendFrame

from raw_pcm_serializer import RawPCMSerializer

load_dotenv()

mock_appointment_counter = 0

# Initialize our continuity manager
continuity = SessionContinuity()

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/create_session")
async def create_session():
    session_id, signature = continuity.new_session()
    return {"session_id": session_id, "signature": signature}

bot_metrics = {
    "reconnect_attempts": 0,
    "successful_resumes": 0,
    "failed_resumes": 0
}

@app.get("/metrics")
async def get_metrics():
    return {
        "bot_metrics": bot_metrics,
        "checkpoint_metrics": continuity.manager.get_metrics()
    }

@app.get("/")
async def get_client(request: Request):
    # We will serve the client from the static directory later.
    # For now, we serve a simple html.
    html_path = os.path.join(os.path.dirname(__file__), "client", "index.html")
    try:
        with open(html_path, "r") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("Client UI not found.")

async def run_bot(websocket_client, session_id: str, is_reconnect: bool = False):
    transport = FastAPIWebsocketTransport(
        websocket=websocket_client,
        params=FastAPIWebsocketParams(
            audio_out_enabled=True,
            add_wav_header=False,
            audio_in_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=16000,
            serializer=RawPCMSerializer(sample_rate=16000),
        )
    )

    llm = GroqLLMService(
        api_key=os.getenv("GROQ_API_KEY"),
        settings=GroqLLMService.Settings(model="llama-3.3-70b-versatile")
    )
    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
    tts = DeepgramTTSService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        voice="aura-asteria-en"
    )

    messages = [
        {"role": "system", "content": "You are a helpful conversational AI assistant. Keep responses brief."}
    ]

    session_pending_tool_calls = {}
    session_completed = False
    is_resumed = False

    async def book_appointment(params, details: str) -> str:
        """
        Book an appointment.
        
        Args:
            details: The details of the appointment to book.
        """
        global mock_appointment_counter
        # Using real tool_call_id instead of hashing details, so we can distinguish 
        # a genuine intentional retry (new tool_call_id) from a resumed duplicate of the same call.
        try:
            call_id = params.tool_call_id
            logger.debug(f"[book_appointment] Started for call_id: {call_id} with details: {details}")
            
            entry = session_pending_tool_calls.get(call_id)
            if entry:
                if entry["status"] == "pending" and is_resumed:
                    logger.debug(f"[book_appointment] Returning SYSTEM_NOTE for pending resumed call {call_id}")
                    result = "SYSTEM_NOTE: The previous attempt to book this appointment was interrupted by a connection drop. The outcome is unknown. Please ask the user if they received a confirmation before retrying."
                    await params.result_callback(result)
                    return result
                elif entry["status"] == "completed":
                    logger.debug(f"[book_appointment] Returning already done for completed call {call_id}")
                    # Do NOT re-execute.
                    result = f"This was already done: {entry['result']}"
                    await params.result_callback(result)
                    return result
            
            # Missing -> Proceed
            session_pending_tool_calls[call_id] = {"status": "pending", "result": None}
            logger.debug(f"[book_appointment] Saving pending state for {call_id}...")
            await continuity.checkpoint(context, session_id, session_pending_tool_calls)
            logger.debug(f"[book_appointment] Saved pending state for {call_id}.")
            
            mock_appointment_counter += 1
            await asyncio.sleep(0.5)
            
            result_str = f"Appointment booked! ID: apt_{mock_appointment_counter}"
            
            session_pending_tool_calls[call_id] = {"status": "completed", "result": result_str}
            logger.debug(f"[book_appointment] Saving completed state for {call_id}...")
            await continuity.checkpoint(context, session_id, session_pending_tool_calls)
            logger.debug(f"[book_appointment] Saved completed state for {call_id}.")
            
            logger.debug(f"[book_appointment] Returning result_str: {result_str}")
            await params.result_callback(result_str)
            return result_str
        except Exception as e:
            logger.error(f"[book_appointment] EXCEPTION: {e}", exc_info=True)
            raise

    async def end_call(params, reason: str) -> str:
        """
        End the call gracefully. Call this tool when the conversation is completely finished, or the user says goodbye.
        
        Args:
            reason: The reason for ending the call.
        """
        nonlocal session_completed
        session_completed = True
        logger.debug("[end_call] Ending the call gracefully via LLM tool.")
        await params.result_callback("Call is ending.")
        from pipecat.frames.frames import EndFrame
        await task.queue_frames([EndFrame()])
        return "Call ended."

    context = LLMContext(messages=messages, tools=[book_appointment, end_call])
    from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
    context_aggregator = LLMContextAggregatorPair(context=context)

    from pipecat.processors.audio.vad_processor import VADProcessor
    from pipecat.audio.vad.silero import SileroVADAnalyzer
    vad = VADProcessor(vad_analyzer=SileroVADAnalyzer())

    pipeline = Pipeline([
        transport.input(),
        vad,
        stt,
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant(),
    ])

    from pipecat.pipeline.worker import PipelineParams
    task = PipelineTask(pipeline, params=PipelineParams(allow_interruptions=True, enable_turn_tracking=True))

    turn_observer = task.turn_tracking_observer
    if turn_observer:
        @turn_observer.event_handler("on_turn_ended")
        async def on_turn_ended(observer, *args, **kwargs):
            # args might be turn_id, duration, was_interrupted
            # Safe checkpointing after each turn
            await continuity.checkpoint(context, session_id, session_pending_tool_calls)

    from pipecat.frames.frames import LLMFullResponseEndFrame

    from datetime import datetime, timezone
    import time
    from loguru import logger
    
    session_start_dt = datetime.now(timezone.utc)
    session_start_ts = time.time()
    
    system_logs = []
    def log_sink(message):
        system_logs.append({
            "timestamp": message.record["time"].isoformat(),
            "level": message.record["level"].name,
            "message": message.record["message"]
        })
    sink_id = logger.add(log_sink)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(t, c):
        print(f"Client connected for session: {session_id}")
        nonlocal is_resumed, session_pending_tool_calls
        is_resumed, session_pending_tool_calls = await continuity.resume_or_start(task, context, session_id)
        
        if is_reconnect:
            bot_metrics["reconnect_attempts"] += 1
            if is_resumed:
                bot_metrics["successful_resumes"] += 1
            else:
                bot_metrics["failed_resumes"] += 1

    logs_dumped = False

    async def dump_logs_to_disk(disposition: str):
        nonlocal logs_dumped
        if logs_dumped:
            return
        logs_dumped = True
        logger.remove(sink_id)
        current_messages = context.get_messages()
        
        # Save to Redis for continuity only if the call wasn't cleanly finished
        if session_completed:
            await continuity.clear(session_id)
            logger.info(f"Cleared context for cleanly finished session {session_id}")
        else:
            await continuity.checkpoint(context, session_id, session_pending_tool_calls)
        
        # Calculate duration
        duration = round(time.time() - session_start_ts, 2)
        
        # Build rich debug log structure
        rich_log = {
            "id": session_id,
            "created_at": session_start_dt.isoformat(),
            "is_completed": session_completed,
            "cost_info": {
                "call_duration_seconds": duration,
                "token_usage_estimated": None 
            },
            "call_type": "inbound",
            "gathered_context": {
                "call_disposition": disposition
            },
            "logs": {
                "transcript": current_messages
            },
            "system_logs": system_logs
        }
        
        import json
        import os
        os.makedirs("src/logs", exist_ok=True)
        log_file = os.path.join("src", "logs", f"{session_id}.json")
        
        # If a log for this session already exists, merge it (happens on reconnect after a drop)
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    existing_log = json.load(f)
                
                # Preserve original start time
                rich_log["created_at"] = existing_log.get("created_at", rich_log["created_at"])
                
                # Combine durations
                prev_duration = existing_log.get("cost_info", {}).get("call_duration_seconds", 0)
                rich_log["cost_info"]["call_duration_seconds"] = round(duration + prev_duration, 2)
                
                # Append system logs chronologically
                rich_log["system_logs"] = existing_log.get("system_logs", []) + rich_log["system_logs"]
            except Exception as e:
                print(f"Warning: Failed to merge existing log for {session_id}: {e}")
                
        with open(log_file, "w") as f:
            json.dump(rich_log, f, indent=2)
        print(f"Saved/Updated unified rich debug log to {log_file}")

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(t, c):
        print(f"Client disconnected for session: {session_id}")
        await dump_logs_to_disk("user_hangup")

    runner = PipelineRunner()
    await runner.run(task)
    
    # Trigger log dump if pipeline gracefully terminated via EndFrame
    await dump_logs_to_disk("graceful_termination")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = websocket.query_params.get("session_id", "test-session")
    signature = websocket.query_params.get("signature", "")
    is_reconnect = websocket.query_params.get("reconnect") == "true"
    
    if not continuity.verify(session_id, signature):
        print(f"WARNING: Invalid signature for session {session_id}. Generating a new one.")
        session_id, _ = continuity.new_session()
        
    await run_bot(websocket, session_id, is_reconnect)


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=True)
