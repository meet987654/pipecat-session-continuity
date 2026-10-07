import pytest
from pipecat_session_continuity.idempotency import (
    canonicalize_arguments,
    generate_idempotency_key,
    IdempotencyRegistry,
)


def test_canonicalize_arguments_dict_ordering():
    args1 = {"date": "2026-10-08", "time": "10:00", "doctor": "Dr. Smith"}
    args2 = {"doctor": "Dr. Smith", "time": "10:00", "date": "2026-10-08"}
    assert canonicalize_arguments(args1) == canonicalize_arguments(args2)


def test_canonicalize_arguments_json_string():
    str1 = '{"time": "10:00", "date": "2026-10-08"}'
    str2 = '{"date": "2026-10-08",   "time": "10:00"}'
    assert canonicalize_arguments(str1) == canonicalize_arguments(str2)


def test_generate_idempotency_key_deterministic():
    args1 = {"action": "book", "id": 123}
    args2 = {"id": 123, "action": "book"}

    key1 = generate_idempotency_key("book_appointment", args1)
    key2 = generate_idempotency_key("book_appointment", args2)
    assert key1 == key2
    assert key1.startswith("idemp:det:book_appointment:")


def test_generate_idempotency_key_client_token():
    key1 = generate_idempotency_key("book_appointment", {"any": "arg"}, client_token="req-abc-999")
    key2 = generate_idempotency_key("book_appointment", {"different": "arg"}, client_token="req-abc-999")
    assert key1 == key2
    assert key1 == "idemp:token:book_appointment:req-abc-999"


def test_idempotency_registry_catches_different_tool_call_ids():
    registry = IdempotencyRegistry()

    # Step 1: Pre-crash call with tool_call_id "call_initial_123"
    args = {"service": "dentist", "time": "2pm"}
    record = registry.register_call(
        tool_name="schedule_visit",
        arguments=args,
        tool_call_id="call_initial_123",
        status="pending"
    )
    assert record["status"] == "pending"

    # Step 2: Post-reconnect LLM generates a COMPLETELY DIFFERENT tool_call_id
    # for the same logical action
    is_dup, existing = registry.check(
        tool_name="schedule_visit",
        arguments=args,
        tool_call_id="call_reconnect_999"  # Brand new LLM-generated ID!
    )
    assert is_dup is True
    assert existing is not None
    assert existing["status"] == "pending"
    assert existing["tool_call_id"] == "call_initial_123"


def test_idempotency_registry_completed_lifecycle():
    registry = IdempotencyRegistry()
    args = {"transfer_amount": 500, "to_account": "ACC_456"}

    # Register
    registry.register_call("transfer_funds", args, tool_call_id="call_1")
    # Complete
    registry.complete_call("call_1", result={"transaction_id": "tx_789", "status": "success"})

    # Check via new tool_call_id
    is_dup, record = registry.check("transfer_funds", args, tool_call_id="call_reconnect_retry")
    assert is_dup is True
    assert record["status"] == "completed"
    assert record["result"]["transaction_id"] == "tx_789"


def test_idempotency_registry_legacy_migration():
    # Legacy format stored in context snapshot
    legacy_data = {
        "call_old_111": {
            "status": "pending",
            "result": None,
            "tool_name": "book_appointment"
        }
    }
    registry = IdempotencyRegistry(legacy_data)
    assert len(registry) == 1

    # Should resolve by call_id
    rec = registry.get_record("call_old_111")
    assert rec is not None
    assert rec["status"] == "pending"
    assert rec["tool_name"] == "book_appointment"
