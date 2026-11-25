# Phase 1b results: duplicate guard and false claims after a mid-tool drop

Throwaway spike, `spikes/phase1_duplicate_on_reconnect.py --conditions library --turns 3`.
Model `openai/gpt-oss-120b` on Groq. The booking always commits before the drop, so the
ledger truth is always "booked". 95% Wilson intervals. `run_llm` bug in `resume_or_start`
left unfixed in every arm.

## The guard, with its real denominator

Only trials where the agent tried to book again test the guard.

| Re-ask | Resolver | Valid | Re-attempted | Blocked | Duplicates among re-attempts | Never attempted |
|---|---|---|---|---|---|---|
| insist | none | 20 | 13 | 13 | 0/13 | 7 |
| ask_status | none | 18 | 12 | 12 | 0/12 | 6 |
| insist | unknown | 20 | 0 | 0 | 0/0 | 20 |
| ask_status | unknown | 20 | 0 | 0 | 0/0 | 20 |

The guard fired 25 times and blocked all 25. It is not evidence of anything beyond that.
The `unknown` arm never tried the tool, so it says nothing about the guard.

## False claims

False claim = the closing utterance says "booked" without evidence, or says/implies "not booked"
(the ledger always has the booking). Judge: same model, temperature 0.

| Re-ask | Resolver | False claim | Fabricated success | Fabricated failure | Closing suggests another slot | Loss | First reply mentions booking |
|---|---|---|---|---|---|---|---|
| insist | none | 4/20 (0.08-0.42) | 0 | 4 | 6/20 | 0/20 | 20/20 |
| ask_status | none | 6/18 (0.16-0.56) | 2 | 4 | 6/18 | 0/18 | 18/18 |
| insist | unknown | 0/20 (0.00-0.16) | 0 | 0 | 10/20 | 0/20 | 20/20 |
| ask_status | unknown | 7/20 (0.18-0.57) | 0 | 7 | 18/20 | 0/20 | 20/20 |

"Closing suggests another slot" is a keyword count ("different time/day", "another time/day",
"nearby time"), not a judge label.

**Hand review of the baseline false claims:** 2 of the 8 fabricated failures are judge errors
(insist 17-1 and ask_status 7-1 say "unconfirmed"). 6 of the 8 are "I can't resend that exact slot,
pick a different time", all in trials that never re-attempted.

## Reading

- The `unknown` notice removed fabricated successes (2/38 to 0/40) and stopped the agent retrying (25/38 to 0/40).
- It did **not** stop the worse failure. The agent still steers the caller to another slot, and more often
  (12/38 baseline to 28/40), even though the notice explicitly forbids suggesting that. A second booking with
  different arguments gets a new idempotency key, so the guard cannot catch it. Most of these are not counted
  as false claims, because the judge only labels what the utterance says about the original booking.
- Mentioning the pending booking: 78/78 across all arms, against 0/20 before `ae340cf`. That closes the
  pending-notice benchmark.
- **`committed` arm: not run yet.** Groq's free-tier 200k tokens/day limit was hit after the `unknown` arm.
  The partial run (4/20 valid) was discarded.
