"""
Phase 1 spike (throwaway): does a stock Pipecat agent run a tool twice when the call
drops after the side effect commits and the caller reconnects?

NOT run in CI. Needs GROQ_API_KEY (read from .env) and spends tokens.

One trial:
  1. Session 1: caller asks to book a slot. The LLM calls book_appointment, which calls
     the mock tool server. The server records the booking (side effect committed), then
     drops the call: the pipeline task is cancelled and the result never returns.
  2. Session 2: a new pipeline (what a redial gets). The caller repeats the request.
  3. The mock server's ledger, not the agent, decides: 2+ bookings for the same slot
     is a duplicate.

Conditions (the only thing that differs is what session 2 starts from):
  stock    : fresh context. Stock Pipecat keeps nothing across a disconnect.
  naive    : the developer snapshots context.get_messages() on disconnect and restores it.
  library  : pipecat-session-continuity: checkpoint + resume_or_start, and the tool
             handler checks the idempotency registry before calling the server.

The drop is triggered by the mock server, not by code in the agent, so the agent's
tool handler is unmodified apart from the library condition's guard.

Usage:
  .venv/Scripts/python spikes/phase1_duplicate_on_reconnect.py --trials 20
  flags: --conditions stock,naive,library  --model openai/gpt-oss-120b  --reask insist|ask_status
"""
import argparse
import asyncio
import json
import math
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from loguru import logger

from pipecat.frames.frames import (
    ErrorFrame,
    FunctionCallInProgressFrame,
    LLMFullResponseEndFrame,
    LLMMessagesAppendFrame,
    LLMTextFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.services.groq.llm import GroqLLMService
from pipecat.services.llm_service import FunctionCallParams

from pipecat_session_continuity import SessionContinuity

load_dotenv()
logger.remove()
logger.add(sys.stderr, level="WARNING")

SYSTEM = (
    "You are a voice assistant for a dental clinic. You book appointments with the "
    "book_appointment tool. Keep replies to one or two short spoken sentences."
)
FIRST_ASK = "Hi, please book me a teeth cleaning this Thursday at 3pm. My name is Priya Shah."
REASKS = {
    # The caller restates the request after redialling: the realistic worst case.
    "insist": "Sorry, the line cut out. I need that teeth cleaning booked for Thursday at 3pm, name Priya Shah.",
    # The caller only asks whether it went through.
    "ask_status": "Sorry, the line cut out. Did my Thursday 3pm cleaning get booked?",
}
# Turn 3 (--turns 3): the caller answers the agent's "did you get a confirmation?" question.
ANSWER = "No, I didn't get anything. Please book it."
JUDGE_LABELS = ("SAYS_BOOKED", "SAYS_NOT_BOOKED", "UNCONFIRMED", "NO_STATEMENT")
JUDGE_PROMPT = (
    "A voice agent for a dental clinic was asked to book a Thursday 3pm cleaning. Below is the agent's "
    "final utterance to the caller. What does it tell the caller about whether that appointment is booked?\n"
    "SAYS_BOOKED: states or implies it is booked/confirmed/all set.\n"
    "SAYS_NOT_BOOKED: states or implies it is not booked, failed, or that no booking exists.\n"
    "UNCONFIRMED: says it cannot confirm, is unsure, or is checking.\n"
    "NO_STATEMENT: says nothing about the booking's status (e.g. only asks a question).\n"
    "Answer with exactly one label.\n\nUtterance:\n"
)


# ---------------------------------------------------------------- mock tool server
class MockToolServer:
    """Records every booking. The first commit of a trial fires the drop."""

    def __init__(self):
        self.ledger = []
        self.drop_armed = True
        self.dropped = asyncio.Event()

    async def book(self, patient_name, day, slot_time):
        self.ledger.append(
            {"patient_name": patient_name, "day": day, "slot_time": slot_time, "t": time.time()}
        )  # side effect committed
        confirmation = f"CONF-{len(self.ledger):03d}"
        if self.drop_armed:
            self.drop_armed = False
            self.dropped.set()
            await asyncio.Event().wait()  # the result never returns: the call is gone
        return {"status": "booked", "confirmation": confirmation}

    def bookings_for_one_slot(self):
        """Max number of bookings that share a business key (day + time)."""
        counts = {}
        for b in self.ledger:
            key = (norm(b["day"]), norm_time(b["slot_time"]))
            counts[key] = counts.get(key, 0) + 1
        return max(counts.values(), default=0)


def norm(s):
    return str(s).strip().lower()


def norm_time(s):
    s = norm(s).replace(" ", "").replace(".", "")
    for a, b in (("3pm", "15:00"), ("3:00pm", "15:00"), ("15:00:00", "15:00"), ("1500", "15:00")):
        if s == a:
            return b
    return s


# ---------------------------------------------------------------- pipeline taps
@dataclass
class Tap:
    response_ends: int = 0
    tool_calls: int = 0
    errors: list = field(default_factory=list)
    text: list = field(default_factory=list)
    responses: list = field(default_factory=list)  # one entry per LLM response, verbatim
    _current: list = field(default_factory=list)
    last_activity: float = field(default_factory=time.time)


class TapProcessor(FrameProcessor):
    def __init__(self, tap: Tap, counts: bool):
        super().__init__()
        self.tap = tap
        self.counts = counts  # only the tap after the LLM counts, and only downstream frames

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, ErrorFrame):
            self.tap.errors.append(str(frame.error))
        elif not self.counts or direction != FrameDirection.DOWNSTREAM:
            pass
        elif isinstance(frame, LLMFullResponseEndFrame):
            self.tap.response_ends += 1
            self.tap.responses.append("".join(self.tap._current))
            self.tap._current = []
            self.tap.last_activity = time.time()
        elif isinstance(frame, FunctionCallInProgressFrame):
            self.tap.tool_calls += 1
            self.tap.last_activity = time.time()
        elif isinstance(frame, LLMTextFrame):
            self.tap.text.append(frame.text)
            self.tap._current.append(frame.text)
            self.tap.last_activity = time.time()
        await self.push_frame(frame, direction)


# ---------------------------------------------------------------- one session
class Session:
    def __init__(self, model, server, messages, handler):
        self.tap = Tap()
        self.context = LLMContext(messages=messages, tools=[handler])
        agg = LLMContextAggregatorPair(context=self.context)
        llm = GroqLLMService(
            api_key=os.environ["GROQ_API_KEY"], settings=GroqLLMService.Settings(model=model)
        )
        pipeline = Pipeline([TapProcessor(self.tap, False), agg.user(), llm, TapProcessor(self.tap, True), agg.assistant()])
        self.task = PipelineTask(pipeline, enable_rtvi=False, idle_timeout_secs=None, check_dangling_tasks=False)
        self.runner = PipelineRunner(handle_sigint=False, check_dangling_tasks=False)
        self._run = None

    async def start(self):
        self._run = asyncio.create_task(self.runner.run(self.task))
        await asyncio.sleep(0.3)

    async def say(self, text):
        await self.task.queue_frames([LLMMessagesAppendFrame([{"role": "user", "content": text}], run_llm=True)])

    async def wait_ends(self, n, timeout):
        t0 = time.time()
        while self.tap.response_ends < n:
            if time.time() - t0 > timeout or self.tap.errors:
                return False
            await asyncio.sleep(0.1)
        return True

    async def wait_settled(self, ends_before, tool_calls_before, timeout=90, quiet=2.0):
        """Settled = every tool call this turn got its follow-up response, then `quiet` s of silence."""
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.tap.errors:
                return False
            new_calls = self.tap.tool_calls - tool_calls_before
            new_ends = self.tap.response_ends - ends_before
            if new_ends >= 1 + new_calls and time.time() - self.tap.last_activity > quiet:
                return True
            await asyncio.sleep(0.1)
        return False

    async def stop(self):
        await self.task.cancel()
        try:
            await asyncio.wait_for(self._run, 20)
        except Exception:
            pass


# ---------------------------------------------------------------- one trial
async def run_trial(condition, model, reask, db_dir, idx, turns=2):
    server = MockToolServer()
    continuity = SessionContinuity(db_path=os.path.join(db_dir, f"t{idx}.db"), secret="spike")
    session_id = f"spike-{condition}-{idx}"
    pending = {}
    guarded = condition == "library"
    suppressed = []
    results_returned = []  # every result the agent actually received from the tool

    async def book_appointment(params: FunctionCallParams, patient_name: str, day: str, slot_time: str):
        """Book a dental appointment.

        Args:
            patient_name: The patient's full name.
            day: Day of the week, for example "Thursday".
            slot_time: Time in 24-hour HH:MM format, for example "15:00".
        """
        args = {"patient_name": patient_name, "day": norm(day), "slot_time": norm_time(slot_time)}
        if guarded:
            is_dup, rec = continuity.check_tool_idempotency(pending, "book_appointment", arguments=args)
            if is_dup:
                suppressed.append(rec.get("status"))
                blocked = {"status": "unconfirmed_duplicate_blocked",
                           "detail": "An identical booking was already sent before the disconnect; its outcome is unconfirmed."}
                results_returned.append(blocked)
                await params.result_callback(blocked)
                return
            continuity.record_tool_call(pending, "book_appointment", arguments=args, tool_call_id=params.tool_call_id)
        result = await server.book(patient_name, day, slot_time)
        if guarded:
            continuity.complete_tool_call(pending, params.tool_call_id, result)
        results_returned.append(result)
        await params.result_callback(result)

    out = {"condition": condition, "trial": idx, "valid": False}

    # Session 1: ask, tool commits, call drops.
    s1 = Session(model, server, [{"role": "system", "content": SYSTEM}], book_appointment)
    await s1.start()
    await s1.say(FIRST_ASK)
    try:
        await asyncio.wait_for(server.dropped.wait(), 60)
    except asyncio.TimeoutError:
        out["reason"] = "no_tool_call_in_session_1" if not s1.tap.errors else f"error: {s1.tap.errors[:1]}"
        out["s1_text"] = "".join(s1.tap.text)
        await s1.stop()
        return out
    await asyncio.sleep(0.3)  # the transport notices the drop a moment later
    snapshot = json.loads(json.dumps(s1.context.get_messages(), default=str))
    if guarded:
        await continuity.checkpoint(s1.context, session_id, pending)
    await s1.stop()
    out["snapshot_has_in_progress_tool"] = any(
        m.get("role") == "tool" and m.get("content") == "IN_PROGRESS" for m in snapshot if isinstance(m, dict)
    )

    # Session 2: the redial.
    if condition == "naive":
        start_msgs = snapshot
    else:
        start_msgs = [{"role": "system", "content": SYSTEM}]
    s2 = Session(model, server, start_msgs, book_appointment)
    await s2.start()
    if guarded:
        pending_restored = None
        is_resumed, pending_restored = await continuity.resume_or_start(s2.task, s2.context, session_id)
        pending.clear()
        pending.update(pending_restored)
        out["resumed"] = is_resumed
        # resume_or_start queues a bridge message; let the agent answer it first.
        await s2.task.queue_frames([LLMMessagesAppendFrame([], run_llm=True)])
        await s2.wait_settled(0, 0, timeout=45)
    ends0, calls0 = s2.tap.response_ends, s2.tap.tool_calls
    await s2.say(REASKS[reask])
    settled = await s2.wait_settled(ends0, calls0)
    if turns >= 3 and settled and not s2.tap.errors:
        ends1, calls1 = s2.tap.response_ends, s2.tap.tool_calls
        await s2.say(ANSWER)
        settled = await s2.wait_settled(ends1, calls1)
    await s2.stop()

    out["turns"] = turns
    out["s2_responses"] = s2.tap.responses + (["".join(s2.tap._current)] if s2.tap._current else [])
    spoken = [t for t in out["s2_responses"] if t.strip()]
    out["closing_utterance"] = spoken[-1] if spoken else ""
    out["tool_results_returned"] = results_returned
    out["booked_result_seen"] = any(r.get("status") == "booked" for r in results_returned)

    out["s2_text"] = "".join(s2.tap.text)
    out["ledger"] = [{k: v for k, v in b.items() if k != "t"} for b in server.ledger]
    out["max_bookings_same_slot"] = server.bookings_for_one_slot()
    out["duplicate"] = out["max_bookings_same_slot"] >= 2
    out["guard_suppressed"] = len(suppressed)
    out["booking_made"] = len(server.ledger) >= 1
    if s2.tap.errors:
        out["reason"] = f"error in session 2: {s2.tap.errors[:1]}"
    elif not settled:
        out["reason"] = "session 2 did not settle"
    else:
        out["valid"] = True
    return out


async def judge(model, utterance):
    """Labels what the closing utterance claims. Groq, temperature 0."""
    if not utterance.strip():
        return "NO_STATEMENT"
    from openai import AsyncOpenAI, RateLimitError

    client = AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=os.environ["GROQ_API_KEY"])
    for attempt in range(8):
        try:
            r = await client.chat.completions.create(
                model=model, temperature=0, max_tokens=1024,
                messages=[{"role": "user", "content": JUDGE_PROMPT + utterance}],
            )
            text = (r.choices[0].message.content or "").upper()
            for label in ("SAYS_NOT_BOOKED", "SAYS_BOOKED", "UNCONFIRMED", "NO_STATEMENT"):
                if label in text:
                    return label
            return "UNPARSED"
        except RateLimitError:
            await asyncio.sleep(3 * (attempt + 1))
    return "JUDGE_FAILED"


def false_claim(r):
    """The agent asserted an outcome the ledger contradicts, or one it had no evidence for.

    Ledger truth: booking_made. Evidence: a "booked" result actually returned to the agent.
    UNCONFIRMED and NO_STATEMENT are never false claims.
    """
    label = r.get("judge_label")
    if label == "SAYS_NOT_BOOKED":
        return r["booking_made"]  # contradicts the ledger
    if label == "SAYS_BOOKED":
        return (not r["booking_made"]) or (not r["booked_result_seen"])  # false, or unsupported
    return False


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--conditions", default="stock,naive,library")
    ap.add_argument("--model", default="openai/gpt-oss-120b")
    ap.add_argument("--reask", default="insist", choices=sorted(REASKS))
    ap.add_argument("--turns", type=int, default=2, choices=(2, 3),
                    help="2 = ask, drop, re-ask (first spike). 3 = also answer the agent's question.")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    if not os.environ.get("GROQ_API_KEY"):
        sys.exit("GROQ_API_KEY is not set (looked in the environment and .env)")

    out_path = a.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), f"phase1{'b' if a.turns == 3 else ''}_results_{a.reask}.json")
    results = []
    with tempfile.TemporaryDirectory() as db_dir:
        for cond in a.conditions.split(","):
            for i in range(a.trials):
                attempts = 0
                while True:
                    attempts += 1
                    r = await run_trial(cond, a.model, a.reask, db_dir, f"{i}-{attempts}", a.turns)
                    r["attempts"] = attempts
                    if r["valid"] or attempts >= 3:
                        break
                    await asyncio.sleep(5)
                if r["valid"]:
                    r["judge_label"] = await judge(a.model, r["closing_utterance"])
                    r["false_claim"] = false_claim(r)
                results.append(r)
                flag = "DUP" if r.get("duplicate") else ("ok " if r["valid"] else "INV")
                print(f"{cond:8s} trial {i:2d}: {flag}  bookings={r.get('max_bookings_same_slot')}  guard={r.get('guard_suppressed')}  judge={r.get('judge_label')}  {r.get('reason', '')}", flush=True)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump({"model": a.model, "reask": a.reask, "turns": a.turns, "results": results}, f, indent=2)
                await asyncio.sleep(1)

    print(f"\nmodel={a.model}  reask={a.reask}  turns={a.turns}")
    metrics = (("duplicate", lambda r: r["duplicate"]),
               ("guard engaged", lambda r: r["guard_suppressed"] > 0),
               ("loss", lambda r: not r["booking_made"]),
               ("false claim", lambda r: r.get("false_claim")))
    for cond in a.conditions.split(","):
        rs = [r for r in results if r["condition"] == cond]
        v = [r for r in rs if r["valid"]]
        print(f"\n{cond}: {len(v)} valid, {len(rs) - len(v)} invalid")
        for name, fn in metrics:
            k = sum(1 for r in v if fn(r))
            lo, hi = wilson(k, len(v))
            print(f"  {name:14s} {k:2d}/{len(v):<2d}  {lo:.2f}-{hi:.2f}")
        labels = {}
        for r in v:
            labels[r.get("judge_label")] = labels.get(r.get("judge_label"), 0) + 1
        print(f"  judge labels   {labels}")
        if v and not any(r["guard_suppressed"] for r in v) and cond == "library":
            print("  ** guard never engaged: the duplicate result says nothing about the guard **")
    print(f"\nper-trial detail: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
