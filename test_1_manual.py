import json
import logging
from src.session_manager import SessionContinuityManager

logging.basicConfig(level=logging.INFO)

def main():
    print("=== Running Test 1: Basic Context Survival ===")
    
    # 1. Start Mock Redis connection (so this runs even if docker isn't up)
    from unittest.mock import MagicMock
    import redis
    
    mock_redis = MagicMock()
    mock_redis_data = {}
    mock_redis.setex = lambda k, t, v: mock_redis_data.update({k: v})
    mock_redis.get = lambda k: mock_redis_data.get(k)
    redis.Redis.from_url = MagicMock(return_value=mock_redis)

    try:
        manager = SessionContinuityManager(redis_url="redis://localhost:6379", ttl_seconds=60)
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    session_id = "test_session_48213"
    
    # 2. Simulate conversation (Turn 1 & 2)
    original_messages = [
        {"role": "system", "content": "You are a helpful conversational AI assistant. Keep responses brief."},
        {"role": "user", "content": "Hi, my order number is 48213."},
        {"role": "assistant", "content": "Got it. Your order number is 48213. How can I help you with it?"}
    ]
    
    print("\n--- SIMULATING DISCONNECT ---")
    # Simulate on_client_disconnected event which saves the context
    manager.save_context(session_id, original_messages)
    
    # 3. Simulate Reconnect
    print("\n--- SIMULATING RECONNECT ---")
    restored_messages = manager.load_context(session_id)
    
    if restored_messages:
        messages = restored_messages
        messages.append({
            "role": "system", 
            "content": "The call was briefly disconnected and has now resumed. Acknowledge this briefly to the user."
        })
        
        print("\n=== RESTORED CONTEXT (What the LLM will see) ===")
        print(json.dumps(messages, indent=2))
        
        # Validate no duplicate initial system prompts
        system_prompts = [m for m in messages if m["role"] == "system"]
        if len(system_prompts) > 2:
            print("\nWARNING: Possible duplicate system prompts found!")
        else:
            print("\nSUCCESS: System prompts look correct (Initial + Bridge).")
            
        # Validate order number survived
        if any("48213" in m.get("content", "") for m in messages):
            print("SUCCESS: Context (order number 48213) survived the roundtrip!")
        else:
            print("FAILED: Context lost.")
    else:
        print("FAILED: No restored messages found.")

if __name__ == "__main__":
    main()
