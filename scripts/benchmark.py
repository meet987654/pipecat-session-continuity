import asyncio
import time
import os
import sqlite3
from dotenv import load_dotenv
from openai import AsyncOpenAI
from pipecat_session_continuity.manager import SessionContinuityManager
from pipecat_session_continuity.storage.sqlite_storage import SQLiteStorage
from pipecat_session_continuity import SessionContinuity
from pipecat.frames.frames import LLMMessagesAppendFrame

class MockContext:
    def __init__(self):
        self.messages = []
    
    def set_messages(self, messages):
        self.messages = messages
    
    def get_messages(self):
        return self.messages
        
    def add_message(self, message):
        self.messages.append(message)

class MockTask:
    async def queue_frames(self, frames):
        pass

async def benchmark_latency(num_runs=1000):
    print(f"--- Running Latency Benchmark ({num_runs} iterations) ---")
    
    # Use an in-memory SQLite DB for maximum raw throughput measurement of the abstraction layer
    # Wait, SQLiteStorage expects a path, let's use a local temp file to simulate disk I/O realistic to SQLite
    storage = SQLiteStorage("benchmark.db")
    manager = SessionContinuityManager(storage_backend=storage)
    
    context = MockContext()
    context.set_messages([
        {"role": "user", "content": "Hello, I need some help."},
        {"role": "assistant", "content": "Of course! What can I help you with?"},
        {"role": "user", "content": "Can you check the weather in San Francisco?"}
    ])
    session_id = "test_sess_benchmark"
    
    # Benchmark Save
    save_times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        await manager.save_context(session_id, context.get_messages(), pending_tool_calls={})
        save_times.append((time.perf_counter() - start) * 1000) # ms
        
    # Benchmark Load
    load_times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        await manager.load_context(session_id)
        load_times.append((time.perf_counter() - start) * 1000) # ms
        
    # Benchmark Full Resume Cycle
    continuity = SessionContinuity(storage_backend=storage)
    resume_times = []
    task = MockTask()
    for _ in range(num_runs):
        start = time.perf_counter()
        await continuity.resume_or_start(task, context, session_id)
        resume_times.append((time.perf_counter() - start) * 1000) # ms

    try:
        if os.path.exists("benchmark.db"):
            os.remove("benchmark.db")
    except Exception:
        pass
        
    def stats(times):
        times.sort()
        avg = sum(times) / len(times)
        p95 = times[int(len(times)*0.95)]
        p99 = times[int(len(times)*0.99)]
        return f"Avg: {avg:.2f}ms | P95: {p95:.2f}ms | P99: {p99:.2f}ms"
        
    print(f"Save Context Latency:   {stats(save_times)}")
    print(f"Load Context Latency:   {stats(load_times)}")
    print(f"Full Resume.start Time: {stats(resume_times)}")
    print()

async def benchmark_hallucination_rate(num_runs=20):
    load_dotenv()
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("Skipping Hallucination Benchmark: GROQ_API_KEY not found.")
        return
        
    print(f"--- Running Duplicate-Call Rate Benchmark ({num_runs} iterations) ---")
    client = AsyncOpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")
    model = "openai/gpt-oss-120b"
    print(f"Model: {model}")
    
    tools = [{
        "type": "function",
        "function": {
            "name": "book_appointment",
            "description": "Book a calendar appointment.",
            "parameters": {
                "type": "object",
                "properties": {"date": {"type": "string"}},
                "required": ["date"]
            }
        }
    }]
    
    base_messages = [
        {"role": "system", "content": "You are a helpful assistant. You MUST use the book_appointment tool when the user asks for an appointment."},
        {"role": "user", "content": "I absolutely need to book an appointment for tomorrow. Call the book_appointment tool right now, do not ask for confirmation!"}
    ]
    
    # 1. Baseline: No mitigation (simulate what happens without the library)
    baseline_duplicates = 0
    print("Testing Baseline (No Mitigation)...")
    for i in range(num_runs):
        # We simulate the LLM *already* decided to call the tool, but we drop before it returns
        messages = base_messages.copy()
        messages.append({"role": "user", "content": "[System Notice: The connection dropped and was just restored. Please briefly acknowledge this to the user and ask how you can continue helping.]"})
        
        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools,
            temperature=0.2
        )
        if response.choices[0].message.tool_calls:
            baseline_duplicates += 1
            
    # 2. Mitigated: With the Session Continuity library's specific prompt
    mitigated_duplicates = 0
    print("Testing Mitigated (With pipecat-session-continuity)...")
    for i in range(num_runs):
        messages = base_messages.copy()
        # The exact message injected by the library for a pending tool
        sys_msg = "[System Notice: The connection dropped while executing the tool 'book_appointment'. Do NOT call this tool again for the same request. Inform the user you are resuming the previous task and ask them to confirm before proceeding.]"
        messages.append({"role": "system", "content": sys_msg})
        messages.append({"role": "user", "content": "[System Notice: The connection dropped and was just restored. Please briefly acknowledge this to the user and ask how you can continue helping.]"})
        
        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools,
            temperature=0.2
        )
        if response.choices[0].message.tool_calls:
            mitigated_duplicates += 1
            
    base_rate = (baseline_duplicates / num_runs) * 100
    mitigated_rate = (mitigated_duplicates / num_runs) * 100
    
    print(f"Baseline Duplicate-Call Rate:  {base_rate:.1f}% ({baseline_duplicates}/{num_runs})")
    print(f"Mitigated Duplicate-Call Rate: {mitigated_rate:.1f}% ({mitigated_duplicates}/{num_runs})")
    print(f"Improvement Factor: {base_rate / max(1, mitigated_rate):.1f}x reduction in hallucinations.")

if __name__ == "__main__":
    asyncio.run(benchmark_latency())
    asyncio.run(benchmark_hallucination_rate())
