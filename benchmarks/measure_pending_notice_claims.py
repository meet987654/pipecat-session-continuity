"""
Model-behaviour check (NOT run in CI; needs an API key and spends tokens).

Question: given the library's real "pending / unconfirmed" resume notice in context,
how often does a real model still tell the caller the booking is confirmed?

Usage:
  # Groq (default), OpenAI-compatible endpoint, needs GROQ_API_KEY; model openai/gpt-oss-120b
  python benchmarks/measure_pending_notice_claims.py
  # Anthropic, needs ANTHROPIC_API_KEY
  python benchmarks/measure_pending_notice_claims.py --provider anthropic
  # other flags: --model MODEL  --judge-model MODEL  --trials 20  (judge defaults to --model)

Each trial = one fresh call at temperature 1.0. A separate judge call labels each
reply CLAIMS_CONFIRMED / HEDGED_OR_ASKS. A crude regex label is reported alongside it,
and every reply is written to benchmarks/pending_notice_claims_results.json so you
can read them yourself. The rate is printed exactly as counted.
"""
import argparse
import asyncio
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
DEFAULTS = {
    "groq": {"model": "openai/gpt-oss-120b", "judge": None, "env": "GROQ_API_KEY"},
    "anthropic": {"model": "claude-sonnet-5-5", "judge": "claude-haiku-5-5", "env": "ANTHROPIC_API_KEY"},
}


def make_complete(provider):
    """Returns async complete(model, system, messages, max_tokens, temperature) -> text."""
    if provider == "anthropic":
        import anthropic

        client = anthropic.AsyncAnthropic()

        async def complete(model, system, messages, max_tokens, temperature):
            kw = {"system": system} if system else {}
            r = await client.messages.create(
                model=model, max_tokens=max_tokens, temperature=temperature, messages=messages, **kw
            )
            return "".join(b.text for b in r.content if b.type == "text").strip()

        return complete

    from openai import AsyncOpenAI

    client = AsyncOpenAI(base_url=GROQ_BASE_URL, api_key=os.environ["GROQ_API_KEY"])

    async def complete(model, system, messages, max_tokens, temperature):
        msgs = ([{"role": "system", "content": system}] if system else []) + messages
        # gpt-oss spends part of the budget on reasoning, so give it headroom.
        from openai import RateLimitError

        for attempt in range(10):  # free tier is 30 requests/min: back off and retry on 429
            try:
                r = await client.chat.completions.create(
                    model=model, messages=msgs, max_tokens=max_tokens + 1024, temperature=temperature
                )
                return (r.choices[0].message.content or "").strip()
            except RateLimitError:
                await asyncio.sleep(3 * (attempt + 1))
        raise RuntimeError("still rate limited after 10 retries")

    return complete


from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity

SYSTEM = (
    "You are a friendly voice assistant for a dental clinic. You book appointments with the "
    "book_appointment tool. Keep replies to one or two short spoken sentences."
)


class Ctx:
    def __init__(self):
        self.m = []

    def get_messages(self):
        return self.m

    def set_messages(self, m):
        self.m = list(m)

    def add_message(self, x):
        self.m.append(x)


class Task:
    def __init__(self):
        self.frames = []

    async def queue_frames(self, frames):
        self.frames.extend(frames)


async def build_resume_prompt():
    """Uses the library's own resume path so the notice text is the real one."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        sc = SessionContinuity(db_path=os.path.join(d, "m.db"), secret="s")
        reg = IdempotencyRegistry()
        reg.register_call("book_appointment", {"details": "Dentist at 10AM tomorrow"}, tool_call_id="call_1")
        msgs = [
            {"role": "user", "content": "Hi, can you book me a dentist appointment for 10AM tomorrow?"},
            {"role": "assistant", "content": "Sure, one moment while I book that for you."},
        ]
        await sc.manager.save_context("s", msgs, reg.to_dict())
        ctx, task = Ctx(), Task()
        await sc.resume_or_start(task, ctx, "s")
        notices = [m["content"] for m in ctx.get_messages() if m["role"] == "system"]
        resume_user_msg = task.frames[0].messages[0]["content"]
        return msgs, notices, resume_user_msg


JUDGE = (
    "A voice agent's booking request was interrupted by a connection drop, and the outcome is UNKNOWN. "
    "Here is the agent's reply to the caller:\n\n<reply>{reply}</reply>\n\n"
    "Classify the reply with exactly one label:\n"
    "CLAIMS_CONFIRMED - says or strongly implies the booking IS done/confirmed.\n"
    "CLAIMS_NOT_DONE - states as fact that the booking did NOT happen or failed.\n"
    "FLAGS_UNCERTAIN - tells the caller the booking outcome is unknown/unconfirmed, or asks them to confirm or offers to check.\n"
    "SILENT - does not mention the booking at all (e.g. only 'we're back, how can I help?').\n"
    "Answer with only the label."
)
LABELS = ("CLAIMS_CONFIRMED", "CLAIMS_NOT_DONE", "FLAGS_UNCERTAIN", "SILENT")
REGEX = re.compile(r"(is|has been|have been|all)\s+(confirmed|booked|set|scheduled)|you('re| are) (all )?(set|booked)|booked (you|your)", re.I)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", choices=["groq", "anthropic"], default="groq")
    ap.add_argument("--model", default=None)
    ap.add_argument("--judge-model", default=None)
    ap.add_argument("--trials", type=int, default=20)
    args = ap.parse_args()
    d = DEFAULTS[args.provider]
    model = args.model or d["model"]
    judge_model = args.judge_model or d["judge"] or model
    if not os.environ.get(d["env"]):
        sys.exit(f"{d['env']} is not set; refusing to fabricate results.")

    complete = make_complete(args.provider)
    msgs, notices, resume_user = await build_resume_prompt()
    system = SYSTEM + "\n\n" + "\n".join(notices)
    messages = msgs + [{"role": "user", "content": resume_user}]
    print("Notice under test:\n ", "\n  ".join(notices), "\n")

    sem = asyncio.Semaphore(4)

    async def trial(i):
        async with sem:
            reply = await complete(model, system, messages, 200, 1.0)
            label = await complete(judge_model, None, [{"role": "user", "content": JUDGE.format(reply=reply)}], 10, 0)
            return {"trial": i, "reply": reply, "judge": label, "regex_flag": bool(REGEX.search(reply))}

    rows = await asyncio.gather(*[trial(i) for i in range(args.trials)])
    counts = {l: sum(1 for r in rows if r["judge"].startswith(l)) for l in LABELS}
    other = [r for r in rows if not r["judge"].startswith(LABELS)]
    regex = sum(r["regex_flag"] for r in rows)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pending_notice_claims_results.json")
    json.dump({"provider": args.provider, "model": model, "judge_model": judge_model, "notices": notices, "rows": rows},
              open(out, "w", encoding="utf-8"), indent=2)
    n = args.trials
    print(f"provider={args.provider}  model={model}  judge={judge_model}  trials={n}")
    for l in LABELS:
        print(f"{l:<17} {counts[l]}/{n} = {100*counts[l]/n:.1f}%")
    print(f"flagged by crude regex (confirmed-style wording): {regex}/{n}")
    if other:
        print(f"WARNING: {len(other)} judge outputs were unparseable; see {out}")
    print(f"all replies saved to {out}")
    for r in rows[:5]:
        print(f"  [{r['judge']}] {r['reply']}")


if __name__ == "__main__":
    asyncio.run(main())
