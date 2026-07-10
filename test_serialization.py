import json
import asyncio
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext

# Mock test for serialization to address the review comment

messages = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "my order number is 48213"}
]
context = OpenAILLMContext(messages=messages)

# 1. Simulate what bot.py does on disconnect
current_messages = context.get_messages()

# 2. Simulate what session_manager.py does
try:
    serialized_data = json.dumps({"messages": current_messages})
    print("SUCCESS: Serialized data successfully:")
    print(serialized_data)
except Exception as e:
    print(f"FAILED to serialize: {e}")

# 3. Simulate rehydration
try:
    parsed_data = json.loads(serialized_data)
    restored_messages = parsed_data.get("messages", [])
    
    new_context = OpenAILLMContext(messages=restored_messages)
    print("SUCCESS: Restored context successfully:")
    print(new_context.get_messages())
except Exception as e:
    print(f"FAILED to restore: {e}")
