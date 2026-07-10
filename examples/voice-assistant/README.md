# Voice Assistant Session Continuity Demo

This is a complete, working example of a Pipecat Voice Agent (FastRTC/WebRTC, Cartesia TTS, Deepgram STT, OpenAI LLM) utilizing `pipecat-session-continuity`.

## Running the Demo

1. Set your API keys in the `.env` file (`OPENAI_API_KEY`, `CARTESIA_API_KEY`, `DEEPGRAM_API_KEY`).
2. Run `python bot.py` to start the backend.
3. Open `http://localhost:8000/client/index.html` in your browser.
4. Click **Connect & Speak** to start the voice session.
5. Click **Simulate Disconnect** to drop the WebSocket connection abruptly.
6. Click **Connect & Speak** again to seamlessly resume the existing conversation!

## Clarification: Client vs Server Drops

The "Simulate Disconnect" button in this demo executes a clean client-side WebSocket drop (`ws.close()`). This demonstrates how the system reacts to standard network disruptions (like switching WiFi networks or driving through a tunnel).

**Important Note on Resilience:** While this demo shows a client drop, the library is explicitly designed to survive **hard OS-level process kills** on the backend worker. During the initial development of this library, we performed a true hard-kill test:
> *"I connected to the agent, started a conversation, and then executed a hard `taskkill /F /PID` on the Python backend process mid-sentence. When I brought the server back up and refreshed the browser, the session continuity layer pulled my state from Redis and successfully resumed the exact context without repeating the intro."*

This demo uses a button for convenience, but the true guarantee lies in the out-of-process storage backends (Redis/SQLite) handling violent server crashes.
