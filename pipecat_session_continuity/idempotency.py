import hashlib
import json
import time
from typing import Any, Dict, Optional, Tuple, Union


def canonicalize_arguments(arguments: Any) -> str:
    """
    Deterministically serialize tool arguments into a normalized JSON string.
    Keys are sorted, whitespaces stripped, and types normalized.
    """
    if arguments is None:
        return "{}"

    if isinstance(arguments, str):
        # If it's already a string, check if it's JSON
        stripped = arguments.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                parsed = json.loads(stripped)
                return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
            except Exception:
                return json.dumps({"_raw": stripped}, sort_keys=True, separators=(",", ":"))
        return json.dumps({"_raw": stripped}, sort_keys=True, separators=(",", ":"))

    if isinstance(arguments, (dict, list)):
        return json.dumps(arguments, sort_keys=True, separators=(",", ":"))

    return json.dumps({"_val": str(arguments)}, sort_keys=True, separators=(",", ":"))


def generate_idempotency_key(
    tool_name: str,
    arguments: Any = None,
    client_token: Optional[str] = None,
) -> str:
    """
    Generates a deterministic idempotency key for a tool call.

    1. If `client_token` is provided, the key is bound directly to the client token
       scoped by tool_name: f"idemp:token:{tool_name}:{client_token}".
    2. Otherwise, the key is deterministically generated from tool_name + SHA-256
       hash of the canonicalized arguments: f"idemp:det:{tool_name}:{hash}".
    """
    tool_clean = tool_name.strip() if tool_name else "unknown"

    if client_token:
        token_clean = str(client_token).strip()
        return f"idemp:token:{tool_clean}:{token_clean}"

    canonical_args = canonicalize_arguments(arguments)
    digest = hashlib.sha256(canonical_args.encode("utf-8")).hexdigest()[:16]
    return f"idemp:det:{tool_clean}:{digest}"


class IdempotencyRegistry:
    """
    Registry that tracks tool execution state across reconnection events.
    Supports dual lookup:
    - By deterministic idempotency key (tool_name + args OR client_token)
    - By raw LLM tool_call_id
    """

    def __init__(self, records: Optional[Dict[str, Dict[str, Any]]] = None):
        # Primary storage: keyed by idempotency_key
        self.records: Dict[str, Dict[str, Any]] = {}
        # Secondary index: tool_call_id -> idempotency_key
        self._call_id_to_key: Dict[str, str] = {}

        if records:
            self._load_from_dict(records)

    def _load_from_dict(self, data: Dict[str, Any]) -> None:
        """
        Loads records supporting both legacy format ({call_id: {status, result, tool_name}})
        and new format with explicit idempotency keys.
        """
        for key, val in data.items():
            if not isinstance(val, dict):
                continue

            idemp_key = val.get("idempotency_key")
            call_id = val.get("tool_call_id") or key
            tool_name = val.get("tool_name", "unknown")
            arguments = val.get("arguments")
            client_token = val.get("client_token")

            if not idemp_key:
                if key.startswith("idemp:"):
                    idemp_key = key
                else:
                    idemp_key = generate_idempotency_key(
                        tool_name=tool_name,
                        arguments=arguments,
                        client_token=client_token,
                    )

            record = {
                "idempotency_key": idemp_key,
                "tool_call_id": call_id,
                "tool_name": tool_name,
                "arguments": arguments,
                "client_token": client_token,
                "status": val.get("status", "pending"),
                "result": val.get("result"),
                "created_at": val.get("created_at", time.time()),
                "updated_at": val.get("updated_at", time.time()),
            }

            self.records[idemp_key] = record
            if call_id:
                self._call_id_to_key[call_id] = idemp_key

    def register_call(
        self,
        tool_name: str,
        arguments: Any = None,
        tool_call_id: Optional[str] = None,
        client_token: Optional[str] = None,
        status: str = "pending",
    ) -> Dict[str, Any]:
        """
        Registers an in-flight tool call.
        Returns the existing record if already present, or creates a new one.
        """
        idemp_key = generate_idempotency_key(
            tool_name=tool_name,
            arguments=arguments,
            client_token=client_token,
        )

        now = time.time()
        if idemp_key in self.records:
            record = self.records[idemp_key]
            if tool_call_id:
                record["tool_call_id"] = tool_call_id
                self._call_id_to_key[tool_call_id] = idemp_key
            record["updated_at"] = now
            return record

        record = {
            "idempotency_key": idemp_key,
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "arguments": arguments,
            "client_token": client_token,
            "status": status,
            "result": None,
            "created_at": now,
            "updated_at": now,
        }

        self.records[idemp_key] = record
        if tool_call_id:
            self._call_id_to_key[tool_call_id] = idemp_key

        return record

    def complete_call(
        self,
        key_or_call_id: str,
        result: Any,
    ) -> Optional[Dict[str, Any]]:
        """
        Marks a tool call as completed and stores the result.
        Accepts either an idempotency key or a tool_call_id.
        """
        idemp_key = self._resolve_key(key_or_call_id)
        if not idemp_key or idemp_key not in self.records:
            return None

        record = self.records[idemp_key]
        record["status"] = "completed"
        record["result"] = result
        record["updated_at"] = time.time()
        return record

    def fail_call(
        self,
        key_or_call_id: str,
        error: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Marks a tool call as failed.
        """
        idemp_key = self._resolve_key(key_or_call_id)
        if not idemp_key or idemp_key not in self.records:
            return None

        record = self.records[idemp_key]
        record["status"] = "failed"
        record["result"] = error
        record["updated_at"] = time.time()
        return record

    def check(
        self,
        tool_name: str,
        arguments: Any = None,
        tool_call_id: Optional[str] = None,
        client_token: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Checks whether this logical tool call is already registered.
        Returns:
            (is_duplicate, record)
        Checks both:
        1. Explicit tool_call_id match
        2. Deterministic idempotency key match (tool_name + arguments or client_token)
        """
        # 1. Check direct tool_call_id first if provided
        if tool_call_id and tool_call_id in self._call_id_to_key:
            key = self._call_id_to_key[tool_call_id]
            if key in self.records:
                return True, self.records[key]

        # 2. Check deterministic idempotency key
        idemp_key = generate_idempotency_key(
            tool_name=tool_name,
            arguments=arguments,
            client_token=client_token,
        )
        if idemp_key in self.records:
            return True, self.records[idemp_key]

        return False, None

    def get_record(self, key_or_call_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a record by idempotency_key or tool_call_id."""
        idemp_key = self._resolve_key(key_or_call_id)
        if idemp_key and idemp_key in self.records:
            return self.records[idemp_key]
        return None

    def _resolve_key(self, key_or_call_id: str) -> Optional[str]:
        if key_or_call_id in self.records:
            return key_or_call_id
        if key_or_call_id in self._call_id_to_key:
            return self._call_id_to_key[key_or_call_id]
        return None

    def __getitem__(self, key: str) -> Dict[str, Any]:
        rec = self.get_record(key)
        if rec is not None:
            return rec
        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return self._resolve_key(key) is not None

    def get(self, key: str, default: Any = None) -> Any:
        rec = self.get_record(key)
        return rec if rec is not None else default

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Returns the serialized records dictionary for saving in context snapshot."""
        out = dict(self.records)
        for call_id, idemp_key in self._call_id_to_key.items():
            if call_id and call_id not in out and idemp_key in self.records:
                out[call_id] = self.records[idemp_key]
        return out

    def __len__(self) -> int:
        return len(self.records)

    def items(self):
        return self.to_dict().items()

    def keys(self):
        return self.to_dict().keys()

    def values(self):
        return self.to_dict().values()

