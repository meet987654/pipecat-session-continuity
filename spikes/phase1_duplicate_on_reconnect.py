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
         --turns 2|3  --resolver none|committed|unknown|not_committed (library only)
         --first-booking committed|not_committed  --pending-timeout S (library only: set_pending_timeout)
         --judge-during-run (default off: trials record the utterances, judging is deferred)
         --rescore FILE: judge an existing results file (claim, mention, steer), no new trials
         --max-trials-per-run N  --daily-token-cap 200000  --daily-request-cap 1000  --max-wait 900
         --fresh (ignore an existing results file instead of resuming it)

Rate limits (Groq free tier: tokens/day, requests/day, tokens/minute). A trial that hits an API
error is an infrastructure failure, not data: it is not written as a result row. On a per-minute
429 the runner waits the retry time the 429 reports and re-runs the trial. On a per-day 429 it
stops cleanly: the daily window is rolling, so its retry time is when one request fits, not a trial. Results are checkpointed after every trial, and
re-running the same command resumes at the first missing trial. Infra events are kept in the
results file under "infra_events", separately from "results".

First booking (--first-booking):
  committed     : the server commits the booking, then drops the call (Phase 1/1b).
  not_committed : the server drops the call BEFORE the booking commits. Later bookings commit
                  normally. The only way the caller ends up booked is a successful retry.

Resolver arms (library condition, Phase 1c): the app registers a status check that calls the
mock server's booking lookup (GET /bookings/{idempotency_key}, modelled as get_booking()).
  committed : the lookup works and finds the booking, so the agent is told it as fact.
  unknown   : the lookup is unavailable and the resolver returns "unknown".
  not_committed : the lookup works and retry_on_not_found=True, so a 404 lets the agent retry once
                  under the same idempotency key (the NOT_COMMITTED answer).
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
    MetricsFrame,
    FunctionCallInProgressFrame,
    LLMFullResponseEndFrame,
    LLMMessagesAppendFrame,
    LLMTextFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.metrics.metrics import LLMUsageMetricsData
from pipecat.pipeline.task import PipelineParams, PipelineTask
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
STEER_PROMPT = (
    "A voice agent for a dental clinic was asked to book a cleaning for Thursday at 3pm. Below is the agent's "
    "final utterance to the caller. Does the agent propose booking a DIFFERENT slot instead of the requested "
    "Thursday 3pm one (a different time, a different day, a nearby time, or asking the caller to pick another "
    "time or day)?\n"
    "STEERS: it proposes or asks the caller to choose a different day or time.\n"
    "NO_STEER: it does not (offering to retry the same Thursday 3pm slot is NO_STEER).\n"
    "Answer with exactly one label.\n\nUtterance:\n"
)
MENTION_PROMPT = (
    "A voice agent for a dental clinic had a call drop while it was booking a Thursday 3pm cleaning. "
    "Below is the first thing the agent said after the caller reconnected. Does it mention that "
    "booking or booking request at all (in any way, including saying it is unsure about it)?\n"
    "MENTIONS: it refers to the booking, the appointment, or the interrupted request.\n"
    "SILENT: it does not (e.g. only welcomes the caller back or asks how it can help).\n"
    "Answer with exactly one label.\n\nUtterance:\n"
)
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


# ---------------------------------------------------------------- rate limits and usage
USAGE = {"tokens": 0, "requests": 0}  # trials run one at a time, so a process-wide counter is enough


class HarnessError(Exception):
    """The trial crashed in our code (tool handler), not in the API: stop the batch."""


class RateLimited(Exception):
    def __init__(self, retry_after_s, detail):
        super().__init__(detail)
        self.retry_after_s = retry_after_s
        # Groq's daily caps are rolling windows: "retry in 2m" means the next request fits, not a
        # whole ~3.5k-token trial. Retrying would start trials that die halfway and burn tokens.
        self.daily = is_daily_cap(detail)


def is_daily_cap(text):
    t = str(text)
    return "per day" in t or "(TPD)" in t or "(RPD)" in t


def redact(text):
    import re
    return re.sub(r"org_[A-Za-z0-9]+", "org_REDACTED", str(text))


def retry_after_from(text, headers=None):
    """Seconds to wait, from the retry-after header or the 429 body's "try again in 1m55.3s"."""
    import re
    if headers and headers.get("retry-after"):
        try:
            return float(headers["retry-after"])
        except ValueError:
            pass
    m = re.search(r"try again in ((?P<h>\d+)h)?((?P<m>\d+)m(?!s))?((?P<s>[\d.]+)s)?((?P<ms>[\d.]+)ms)?", str(text))
    if not m or not any(m.group(g) for g in ("h", "m", "s", "ms")):
        return None
    g = lambda k: float(m.group(k) or 0)
    return g("h") * 3600 + g("m") * 60 + g("s") + g("ms") / 1000


def is_rate_limit(text):
    t = str(text)
    return "429" in t or "rate_limit" in t or "Rate limit" in t


# ---------------------------------------------------------------- mock tool server
class MockToolServer:
    """Records every booking. The first commit of a trial fires the drop."""

    def __init__(self, status_api_up=True, first_commits=True):
        self.first_commits = first_commits
        self.ledger = []
        self.drop_armed = True
        self.dropped = asyncio.Event()
        self.status_api_up = status_api_up
        self.by_key = {}  # idempotency_key -> booking, for the status endpoint

    async def book(self, patient_name, day, slot_time, idempotency_key=None):
        if self.drop_armed and not self.first_commits:
            self.drop_armed = False
            self.dropped.set()
            await asyncio.Event().wait()  # dropped before the write: nothing committed
        confirmation = f"CONF-{len(self.ledger) + 1:03d}"
        self.ledger.append(
            {"patient_name": patient_name, "day": day, "slot_time": slot_time, "t": time.time()}
        )  # side effect committed
        if idempotency_key:
            self.by_key[idempotency_key] = {"status": "booked", "confirmation": confirmation,
                                            "day": day, "slot_time": slot_time}
        if self.drop_armed:
            self.drop_armed = False
            self.dropped.set()
            await asyncio.Event().wait()  # the result never returns: the call is gone
        return {"status": "booked", "confirmation": confirmation}

    async def get_booking(self, idempotency_key):
        """GET /bookings/{idempotency_key}: 200 + booking, 404 (None), or 503 when the API is down."""
        if not self.status_api_up:
            raise ConnectionError("503 status API unavailable")
        return self.by_key.get(idempotency_key)

    def requested_slot_booked(self):
        return any(norm(b["day"]) == "thursday" and norm_time(b["slot_time"]) == "15:00" for b in self.ledger)

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
        elif self.counts and isinstance(frame, MetricsFrame):
            for d in frame.data:
                if isinstance(d, LLMUsageMetricsData):
                    USAGE["tokens"] += d.value.total_tokens or 0
                    USAGE["requests"] += 1
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
        self.task = PipelineTask(pipeline, params=PipelineParams(enable_metrics=True, enable_usage_metrics=True),
                                 enable_rtvi=False, idle_timeout_secs=None, check_dangling_tasks=False)
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
async def run_trial(condition, model, reask, db_dir, idx, turns=2, resolver="none", first_booking="committed",
                    pending_timeout=None):
    server = MockToolServer(status_api_up=(resolver != "unknown"), first_commits=(first_booking == "committed"))
    continuity = SessionContinuity(db_path=os.path.join(db_dir, f"t{idx}.db"), secret="spike")
    if pending_timeout is not None:
        # Every timeout arm so far measured the permitted retry (the pre-0.4.0 default).
        continuity.set_pending_timeout("book_appointment", pending_timeout, allow_unverified_retry=True)
    resolver_answers = []
    if condition == "library" and resolver != "none":
        async def check_booking(idempotency_key, args):
            try:
                booking = await server.get_booking(idempotency_key)
            except ConnectionError:
                resolver_answers.append("unknown")
                return "unknown"
            if booking is None:
                resolver_answers.append("not_found")
                return "not_found"
            resolver_answers.append("completed")
            return ("completed", booking)

        continuity.register_status_check("book_appointment", check_booking,
                                         retry_on_not_found=(resolver == "not_committed"))
    session_id = f"spike-{condition}-{idx}"
    pending = {}
    guarded = condition == "library"
    suppressed = []
    retried_after_not_found = []
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
            if is_dup and rec.get("status") == "retry_allowed":
                is_dup = False  # a status check proved it did not happen: the one permitted retry
            if not is_dup and rec and rec.get("status") in ("retry_allowed", "unresolved"):
                retried_after_not_found.append(rec["status"])  # unresolved: past pending_timeout, caller chose
            if is_dup:
                suppressed.append(rec.get("status"))
                if rec.get("status") == "completed" and rec.get("result"):
                    # Already done (here: confirmed by the status check). Return what happened.
                    blocked = rec["result"]
                else:
                    blocked = {"status": "unconfirmed_duplicate_blocked",
                               "detail": "An identical booking was already sent before the disconnect; its outcome is unconfirmed."}
                results_returned.append(blocked)
                await params.result_callback(blocked)
                return
            rec = continuity.record_tool_call(pending, "book_appointment", arguments=args, tool_call_id=params.tool_call_id)
            key = rec.get("idempotency_key") if isinstance(rec, dict) else None
        result = await server.book(patient_name, day, slot_time, idempotency_key=key if guarded else None)
        if guarded:
            continuity.complete_tool_call(pending, params.tool_call_id, result)
        results_returned.append(result)
        await params.result_callback(result)

    out = {"condition": condition, "trial": idx, "valid": False, "resolver": resolver, "first_booking": first_booking,
           "pending_timeout": pending_timeout}
    usage0 = dict(USAGE)

    def infra_check(errors):
        """API errors are infrastructure, not agent behaviour: raise instead of returning a row."""
        for e in errors:
            if is_rate_limit(e):
                raise RateLimited(retry_after_from(e), redact(e)[:400])
        for e in errors:
            if "Error executing function call" in str(e):
                # The tool handler raised (e.g. the model passed an argument the tool does not take).
                # Not a transient API error: retrying would burn tokens on the same crash.
                raise HarnessError(redact(e)[:400])
        if errors:
            raise RateLimited(30.0, redact(errors[0])[:400])  # other API error: short back-off

    # Session 1: ask, tool commits, call drops.
    s1 = Session(model, server, [{"role": "system", "content": SYSTEM}], book_appointment)
    await s1.start()
    await s1.say(FIRST_ASK)
    try:
        await asyncio.wait_for(server.dropped.wait(), 60)
    except asyncio.TimeoutError:
        await s1.stop()
        infra_check(s1.tap.errors)
        out["reason"] = "no_tool_call_in_session_1"
        out["s1_text"] = "".join(s1.tap.text)
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
        out["resume_statuses"] = sorted({v.get("status") for v in pending.values() if isinstance(v, dict)})
        # resume_or_start queues the bridge message with run_llm=True (fixed after Phase 1b; every arm
        # up to phase1b_results_insist_pt0.json ran with an extra empty run_llm nudge here instead).
        await s2.wait_settled(0, 0, timeout=45)
    ends0, calls0 = s2.tap.response_ends, s2.tap.tool_calls
    await s2.say(REASKS[reask])
    settled = await s2.wait_settled(ends0, calls0)
    if turns >= 3 and settled and not s2.tap.errors:
        ends1, calls1 = s2.tap.response_ends, s2.tap.tool_calls
        await s2.say(ANSWER)
        settled = await s2.wait_settled(ends1, calls1)
    await s2.stop()
    infra_check(s2.tap.errors)

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
    out["s2_tool_calls"] = s2.tap.tool_calls
    out["resolver_answers"] = resolver_answers
    out["booking_made"] = server.requested_slot_booked()  # the requested Thursday 15:00 slot exists
    out["other_slot_booked"] = any(not (norm(b["day"]) == "thursday" and norm_time(b["slot_time"]) == "15:00")
                                   for b in server.ledger)
    out["permitted_retries"] = len(retried_after_not_found)
    out["agent_tokens"] = USAGE["tokens"] - usage0["tokens"]
    out["agent_requests"] = USAGE["requests"] - usage0["requests"]
    if not settled:
        out["reason"] = "session 2 did not settle"
    else:
        out["valid"] = True
    return out


async def judge(model, utterance, prompt=None, labels=None, empty="NO_STATEMENT"):
    """Labels what an utterance claims. Groq, temperature 0. Defaults to the closing-claim judge."""
    prompt = prompt or JUDGE_PROMPT
    labels = labels or ("SAYS_NOT_BOOKED", "SAYS_BOOKED", "UNCONFIRMED", "NO_STATEMENT")
    if not utterance.strip():
        return empty
    from openai import AsyncOpenAI, RateLimitError

    client = AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=os.environ["GROQ_API_KEY"])
    for attempt in range(8):
        try:
            USAGE["requests"] += 1
            r = await client.chat.completions.create(
                model=model, temperature=0, max_tokens=1024,
                messages=[{"role": "user", "content": prompt + utterance}],
            )
            USAGE["tokens"] += (r.usage.total_tokens if r.usage else 0)
            text = (r.choices[0].message.content or "").upper()
            for label in labels:
                if label in text:
                    return label
            return "UNPARSED"
        except RateLimitError as e:
            wait = retry_after_from(e, e.response.headers if e.response is not None else None) or 3 * (attempt + 1)
            if wait > JUDGE_MAX_WAIT or is_daily_cap(e):
                raise RateLimited(wait, redact(e)[:400])
            await asyncio.sleep(wait + 1)
    return "JUDGE_FAILED"


JUDGE_MAX_WAIT = 900


def false_claim(r):
    """The agent asserted an outcome the ledger contradicts, or one it had no evidence for.

    Ledger truth: booking_made (the requested slot exists at the end). Evidence: a "booked" result actually returned to the agent, or
    a status check that confirmed it. UNCONFIRMED and NO_STATEMENT are never false claims.
    """
    return false_claim_kind(r) is not None


def false_claim_kind(r):
    """fabricated_success ("booked, CONF-002") or fabricated_failure ("no booking"), else None.

    A fabricated failure is the worse one: the caller goes and books again elsewhere.
    """
    label = r.get("judge_label")
    evidence = r.get("booked_result_seen") or "completed" in r.get("resolver_answers", [])
    if label == "SAYS_NOT_BOOKED" and r["booking_made"]:
        return "fabricated_failure"  # contradicts the ledger
    if label == "SAYS_BOOKED" and ((not r["booking_made"]) or (not evidence)):
        return "fabricated_success"  # false, or unsupported
    return None


def attempted_rebook(r):
    """The agent tried the tool again in session 2 (so the guard was actually tested)."""
    if "s2_tool_calls" in r:
        return r["s2_tool_calls"] > 0
    return r["guard_suppressed"] > 0 or len(r["ledger"]) > 1  # older files: inferred


async def annotate(model, r):
    """Judge labels for a valid trial. The mention judge reads the first post-resume reply."""
    u0 = dict(USAGE)
    if "judge_label" not in r:
        r["judge_label"] = await judge(model, r["closing_utterance"])
    r["false_claim"] = false_claim(r)
    r["false_claim_kind"] = false_claim_kind(r)
    if r["condition"] == "library" and r.get("s2_responses") and "mention_label" not in r:
        first = r["s2_responses"][0]
        r["first_reply"] = first
        r["mention_label"] = await judge(model, first, MENTION_PROMPT, ("MENTIONS", "SILENT"), empty="SILENT")
    if "steer_label" not in r:
        r["steer_label"] = await judge(model, r["closing_utterance"], STEER_PROMPT, ("NO_STEER", "STEERS"), empty="NO_STEER")
    r["judge_tokens"] = r.get("judge_tokens", 0) + USAGE["tokens"] - u0["tokens"]
    r["judge_requests"] = r.get("judge_requests", 0) + USAGE["requests"] - u0["requests"]


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def trial_index(r):
    return r["index"] if "index" in r else int(str(r["trial"]).split("-")[0])


def per_trial_cost(spike_dir, default_tokens=6000, default_requests=8, include_judge=True):
    """Mean tokens and requests per valid trial from recorded results, or a default.
    include_judge=False: agent cost only (judging is deferred to --rescore)."""
    import glob
    rows = []
    for f in glob.glob(os.path.join(spike_dir, "phase1b_results_*.json")):
        with open(f, encoding="utf-8") as fh:
            rows += [r for r in json.load(fh)["results"] if r.get("valid") and "agent_tokens" in r]
    if not rows:
        return default_tokens, default_requests, f"defaults (no recorded usage yet: {default_tokens} tokens, {default_requests} requests)"
    j = 1 if include_judge else 0
    tok = sum(r["agent_tokens"] + j * r.get("judge_tokens", 0) for r in rows) / len(rows)
    req = sum(r["agent_requests"] + j * r.get("judge_requests", 0) for r in rows) / len(rows)
    return tok, req, f"{len(rows)} recorded trials ({'agent + judge' if include_judge else 'agent only'})"


def probe_remaining_requests(model):
    """One max_tokens=1 call (~75 tokens) to read x-ratelimit-remaining-requests."""
    from openai import OpenAI, RateLimitError
    try:
        r = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=os.environ["GROQ_API_KEY"]) \
            .chat.completions.with_raw_response.create(model=model, max_tokens=1, messages=[{"role": "user", "content": "hi"}])
        return int(r.headers.get("x-ratelimit-remaining-requests"))
    except RateLimitError as e:
        wait = retry_after_from(e, e.response.headers if e.response is not None else None)
        print(f"         probe hit a 429 now: {redact(e)[:200]} (retry in {wait}s)")
        return 0
    except Exception:
        return None


def summarize(results, conditions):
    def kind(k):
        return lambda r: r.get("false_claim_kind") == k

    metrics = (("duplicate", lambda r: r["duplicate"]),
               ("attempted", attempted_rebook),
               ("guard engaged", lambda r: r["guard_suppressed"] > 0),
               ("loss", lambda r: not r["booking_made"]),
               ("other slot booked", lambda r: r.get("other_slot_booked", False)),
               ("permitted retry", lambda r: r.get("permitted_retries", 0) > 0),
               ("steers (judge)", lambda r: r.get("steer_label") == "STEERS"),
               ("false claim", lambda r: r.get("false_claim")),
               ("  fab. success", kind("fabricated_success")),
               ("  fab. failure", kind("fabricated_failure")))
    for cond in conditions:
        rs = [r for r in results if r["condition"] == cond]
        v = [r for r in rs if r["valid"]]
        unjudged = sum("steer_label" not in r for r in v)
        print(f"\n{cond}: {len(v)} valid, {len(rs) - len(v)} invalid"
              + (f"  ({unjudged} not judged yet: judge-based rows below undercount; run --rescore)" if unjudged else ""))
        for name, fn in metrics:
            k = sum(1 for r in v if fn(r))
            lo, hi = wilson(k, len(v))
            print(f"  {name:15s} {k:2d}/{len(v):<2d}  {lo:.2f}-{hi:.2f}")
        eng = [r for r in v if attempted_rebook(r)]
        print(f"  duplicates among trials that re-attempted: {sum(r['duplicate'] for r in eng)}/{len(eng)}"
              f"  (blocked: {sum(r['guard_suppressed'] > 0 for r in eng)}/{len(eng)}); never attempted: {len(v) - len(eng)}")
        m = [r for r in v if "mention_label" in r]
        if m:
            k = sum(r["mention_label"] == "MENTIONS" for r in m)
            lo, hi = wilson(k, len(m))
            print(f"  first reply mentions booking {k}/{len(m)}  {lo:.2f}-{hi:.2f}")
        labels = {}
        for r in v:
            labels[r.get("judge_label")] = labels.get(r.get("judge_label"), 0) + 1
        print(f"  judge labels    {labels}")
        if v and not any(r["guard_suppressed"] for r in v) and cond == "library":
            print("  ** guard never engaged: the duplicate result says nothing about the guard **")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--conditions", default="stock,naive,library")
    ap.add_argument("--model", default="openai/gpt-oss-120b")
    ap.add_argument("--reask", default="insist", choices=sorted(REASKS))
    ap.add_argument("--turns", type=int, default=2, choices=(2, 3),
                    help="2 = ask, drop, re-ask (first spike). 3 = also answer the agent's question.")
    ap.add_argument("--first-booking", default="committed", choices=("committed", "not_committed"),
                    help="whether the booking commits before the drop")
    ap.add_argument("--resolver", default="none", choices=("none", "committed", "unknown", "not_committed"),
                    help="library only: status check that resolves the pending booking on resume")
    ap.add_argument("--pending-timeout", type=float, default=None,
                    help="library only: set_pending_timeout(book_appointment, S). 0 = any pending record at resume is past it")
    ap.add_argument("--judge-during-run", action="store_true",
                    help="judge each trial as it finishes (default: record only; judge later with --rescore)")
    ap.add_argument("--rescore", default=None, help="annotate an existing results file instead of running trials")
    ap.add_argument("--out", default=None)
    ap.add_argument("--fresh", action="store_true", help="start over instead of resuming an existing results file")
    ap.add_argument("--max-trials-per-run", type=int, default=None, help="run at most N new trials, then stop")
    ap.add_argument("--daily-token-cap", type=int, default=200_000)
    ap.add_argument("--daily-request-cap", type=int, default=1_000)
    ap.add_argument("--max-wait", type=float, default=900, help="longest rate-limit wait (s) before stopping cleanly")
    ap.add_argument("--over-budget", action="store_true", help="run even if the estimate exceeds the daily caps")
    a = ap.parse_args()
    global JUDGE_MAX_WAIT
    JUDGE_MAX_WAIT = a.max_wait
    if not os.environ.get("GROQ_API_KEY"):
        sys.exit("GROQ_API_KEY is not set (looked in the environment and .env)")

    if a.rescore:
        with open(a.rescore, encoding="utf-8") as f:
            data = json.load(f)
        for r in data["results"]:
            if r["valid"]:
                try:
                    await annotate(data["model"], r)
                except RateLimited as e:
                    print(f"STOPPED cleanly: {redact(e)[:160]}. Rerun --rescore to continue.")
                    break
                with open(a.rescore, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
        summarize(data["results"], sorted({r["condition"] for r in data["results"]}))
        print(f"\nannotated: {a.rescore}")
        return

    suffix = (("" if a.resolver == "none" else f"_{a.resolver}")
              + ("" if a.pending_timeout is None else f"_pt{a.pending_timeout:g}")
              + ("_nc" if a.first_booking == "not_committed" else ""))
    out_path = a.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), f"phase1{'b' if a.turns == 3 else ''}_results_{a.reask}{suffix}.json")
    config = {"model": a.model, "reask": a.reask, "turns": a.turns, "resolver": a.resolver, "first_booking": a.first_booking,
              "pending_timeout": a.pending_timeout}
    results, infra_events = [], []
    if os.path.exists(out_path) and not a.fresh:
        with open(out_path, encoding="utf-8") as f:
            prev = json.load(f)
        defaults = {"resolver": "none", "first_booking": "committed", "pending_timeout": None}
        if any(prev.get(k, defaults.get(k)) != v for k, v in config.items()):
            sys.exit(f"{out_path} was made with different settings; pass --fresh or --out")
        results, infra_events = prev["results"], prev.get("infra_events", [])
        print(f"resuming {out_path}: {len(results)} trials already recorded")

    def done(cond, i):
        return any(r["condition"] == cond and trial_index(r) == i for r in results)

    todo = [(c, i) for c in a.conditions.split(",") for i in range(a.trials) if not done(c, i)]
    if a.max_trials_per_run:
        todo = todo[: a.max_trials_per_run]
    tok, req, src = per_trial_cost(os.path.dirname(out_path), include_judge=a.judge_during_run)
    print(f"budget: {len(todo)} trials x ~{tok:,.0f} tokens = ~{len(todo) * tok:,.0f} tokens "
          f"(daily cap {a.daily_token_cap:,}); ~{len(todo) * req:.0f} requests (daily cap {a.daily_request_cap:,}). "
          f"Per-trial cost from {src}.")
    remaining_req = probe_remaining_requests(a.model)
    if remaining_req is not None:
        print(f"         Groq reports {remaining_req} requests left today. Tokens left today is not reported until a 429.")
    fits = int(min(a.daily_token_cap / tok, (remaining_req if remaining_req is not None else a.daily_request_cap) / req))
    if len(todo) > fits and not a.over_budget:
        sys.exit(f"over budget: ~{fits} trials fit in one day's caps (less if tokens were already used today). "
                 f"Rerun with --max-trials-per-run {fits} (it resumes later), or --over-budget to run anyway.")

    def save():
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({**config, "results": results, "infra_events": infra_events}, f, indent=2)

    stopped = None
    with tempfile.TemporaryDirectory() as db_dir:
        for cond, i in todo:
            attempts = 0  # behavioural re-tries (e.g. no tool call in session 1); infra re-tries don't count
            while True:
                try:
                    r = await run_trial(cond, a.model, a.reask, db_dir, f"{i}-{attempts + 1}", a.turns, a.resolver, a.first_booking,
                                        a.pending_timeout)
                    if r["valid"] and a.judge_during_run:
                        await annotate(a.model, r)
                except HarnessError as e:
                    infra_events.append({"condition": cond, "index": i, "t": time.time(), "kind": "harness_error",
                                         "detail": str(e)})
                    save()
                    stopped = f"harness error (not a rate limit, not data): {e}"
                    break
                except RateLimited as e:
                    infra_events.append({"condition": cond, "index": i, "t": time.time(),
                                         "retry_after_s": e.retry_after_s, "detail": redact(e)})
                    save()
                    wait = e.retry_after_s or 60
                    if e.daily:
                        stopped = (f"daily cap reached ({e.retry_after_s or 0:.0f}s until the next request fits, "
                                   f"not a whole trial; the window is rolling)")
                        break
                    if wait > a.max_wait:
                        stopped = f"rate limit: retry in {wait / 3600:.1f} h (more than --max-wait {a.max_wait}s)"
                        break
                    print(f"{cond:8s} trial {i:2d}: rate limited, waiting {wait:.0f}s (infra, not recorded as a trial)", flush=True)
                    await asyncio.sleep(wait + 2)
                    continue
                attempts += 1
                r["attempts"] = attempts
                if r["valid"] or attempts >= 3:
                    break
                await asyncio.sleep(5)
            if stopped:
                break
            r["index"] = i
            results.append(r)
            save()
            flag = "DUP" if r.get("duplicate") else ("ok " if r["valid"] else "INV")
            print(f"{cond:8s} trial {i:2d}: {flag}  bookings={r.get('max_bookings_same_slot')}  guard={r.get('guard_suppressed')}  judge={r.get('judge_label')}  kind={r.get('false_claim_kind')}  mention={r.get('mention_label')}  tokens={r.get('agent_tokens', 0) + r.get('judge_tokens', 0)}  {r.get('reason', '')}", flush=True)
            await asyncio.sleep(1)

    if stopped:
        print(f"\nSTOPPED cleanly: {stopped}. {len(results)} trials saved; rerun the same command to resume.")
    print(f"\nmodel={a.model}  reask={a.reask}  turns={a.turns}  resolver={a.resolver}  pending_timeout={a.pending_timeout}")
    summarize(results, a.conditions.split(","))
    print(f"\nper-trial detail: {out_path}")
    if any(r.get("valid") and "steer_label" not in r for r in results):
        print(f"judge labels missing for some trials: judge them with --rescore {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
