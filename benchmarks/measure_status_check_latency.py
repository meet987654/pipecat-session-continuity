"""
Measures the resume latency added by status-check hooks.

Compares SessionContinuity.resume_or_start() with (a) no hook and (b) a hook that
answers immediately, then (c) a hook that never answers (worst case = the timeout).
Run:  python benchmarks/measure_status_check_latency.py [N]
"""
import asyncio
import os
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity


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
    async def queue_frames(self, frames):
        pass


async def measure_pair(hook, n, pending=1):
    """Interleaves hook / no-hook resumes so disk and OS noise hits both equally."""
    with tempfile.TemporaryDirectory() as d:
        base_sc = SessionContinuity(db_path=os.path.join(d, "b.db"), secret="s")
        hook_sc = SessionContinuity(db_path=os.path.join(d, "h.db"), secret="s")
        for sc in (base_sc, hook_sc):
            reg = IdempotencyRegistry()
            for i in range(pending):
                reg.register_call("book_appointment", {"details": f"slot {i}"}, tool_call_id=f"c{i}")
            await sc.manager.save_context("s", [], reg.to_dict())
        hook_sc.register_status_check("book_appointment", hook)
        base, hooked = [], []
        for _ in range(n):
            for sc, out in ((base_sc, base), (hook_sc, hooked)):
                t = time.perf_counter()
                await sc.resume_or_start(Task(), Ctx(), "s")
                out.append((time.perf_counter() - t) * 1000)
        return base, hooked


async def measure_hook_step(hook, n, pending=1):
    """Times only SessionContinuity._run_status_checks (no storage I/O)."""
    sc = SessionContinuity(db_path=os.path.join(tempfile.gettempdir(), "sc_bench_unused.db"), secret="s")
    sc.register_status_check("book_appointment", hook)
    out = []
    for _ in range(n):
        reg = IdempotencyRegistry()
        for i in range(pending):
            reg.register_call("book_appointment", {"details": f"slot {i}"}, tool_call_id=f"c{i}")
        t = time.perf_counter()
        await sc._run_status_checks(reg)
        out.append((time.perf_counter() - t) * 1000)
    return out


async def measure_timeout(n=3, timeout=1.0):
    with tempfile.TemporaryDirectory() as d:
        sc = SessionContinuity(db_path=os.path.join(d, "t.db"), secret="s", status_check_timeout_seconds=timeout)
        reg = IdempotencyRegistry()
        reg.register_call("book_appointment", {"details": "x"}, tool_call_id="c")
        await sc.manager.save_context("s", [], reg.to_dict())

        async def never(k, a):
            await asyncio.sleep(3600)

        sc.register_status_check("book_appointment", never)
        out = []
        for _ in range(n):
            t = time.perf_counter()
            await sc.resume_or_start(Task(), Ctx(), "s")
            out.append((time.perf_counter() - t) * 1000)
        return out


def fmt(name, s):
    s = sorted(s)
    return f"{name:<42} mean {statistics.mean(s):7.3f} ms  p50 {s[len(s)//2]:7.3f}  p95 {s[int(len(s)*.95)]:7.3f}"


async def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300

    async def instant(k, a):
        return "unknown"

    await measure_pair(instant, 30)  # warm-up
    for pending in (1, 5):
        base, hooked = await measure_pair(instant, n, pending)
        print(f"-- full resume_or_start, {pending} pending record(s), N={n}, SQLite, interleaved --")
        print(fmt("no hook (baseline)", base))
        print(fmt("instant hook", hooked))
        print(f"   added: median {statistics.median(hooked) - statistics.median(base):+.3f} ms, "
              f"mean {statistics.mean(hooked) - statistics.mean(base):+.3f} ms")
    for pending in (1, 5):
        print(fmt(f"hook step only, {pending} pending (no storage I/O)", await measure_hook_step(instant, n, pending)))
    t = await measure_timeout()
    print(f"hook that never answers, 3 runs: {[round(x) for x in t]} ms (bounded by the 1000 ms timeout)")


if __name__ == "__main__":
    asyncio.run(main())
