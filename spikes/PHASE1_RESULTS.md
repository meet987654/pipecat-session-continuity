# Phase 1b results: guard, false claims and steering after a mid-tool drop

Throwaway spike, `spikes/phase1_duplicate_on_reconnect.py --conditions library --turns 3`.
Model `openai/gpt-oss-120b` on Groq, all arms. 95% Wilson intervals. The `run_llm` bug in
`resume_or_start` is left unfixed in every arm. In every trial so far the first booking
**committed** before the drop (`--first-booking committed`), so the ledger truth is always "booked".

## Headline: the `unknown` notice switches off the retry, and with it the guard

| Caller script | Resolver | Guard engaged | Duplicates |
|---|---|---|---|
| insist | none | 13/20 (0.43-0.82) | 0/13 re-attempts (0.00-0.23) |
| insist | unknown | **0/20 (0.00-0.16)** | 0/0 re-attempts: untested |
| ask_status | none | 12/18 (0.44-0.84) | 0/12 re-attempts (0.00-0.24) |
| ask_status | unknown | **0/20 (0.00-0.16)** | 0/0 re-attempts: untested |

In the baseline the guard fired 25 times and blocked all 25. In the `unknown` arm the agent never
tried the tool again, so **its zero duplicates do not test the guard at all.** Guard engagement is the
difference whose intervals clearly do not overlap, within each caller script.

## False claims, judge vs second rater

False claim = says "booked" without evidence, or says/implies "not booked" (the booking always exists).
Two label sources: the LLM judge (same model, temperature 0) and a second rater (see "Judge validation").

| Caller script | Resolver | False claim, judge | False claim, rater | Fab. success, rater | Fab. failure, rater | Loss |
|---|---|---|---|---|---|---|
| insist | none | 4/20 (0.08-0.42) | 3/20 (0.05-0.36) | 0/20 (0.00-0.16) | 3/20 (0.05-0.36) | 0/20 (0.00-0.16) |
| insist | unknown | 0/20 (0.00-0.16) | 0/20 (0.00-0.16) | 0/20 (0.00-0.16) | 0/20 (0.00-0.16) | 0/20 (0.00-0.16) |
| ask_status | none | 6/18 (0.16-0.56) | 5/18 (0.12-0.51) | 2/18 (0.03-0.33) | 3/18 (0.06-0.39) | 0/18 (0.00-0.18) |
| ask_status | unknown | 7/20 (0.18-0.57) | 4/20 (0.08-0.42) | 0/20 (0.00-0.16) | 4/20 (0.08-0.42) | 0/20 (0.00-0.16) |

Loss is 0 by construction: the booking always commits. It becomes measurable only with
`--first-booking not_committed` (below).

## Steering the caller to a different slot

Rater labels for all 78, rubric in `spikes/gold/RUBRIC.md`. The LLM steering judge has labelled only
insist/none (20/20) and 3 ask_status/none closings so far (daily token cap).

| Caller script | Resolver | Steers, rater | Steers, judge |
|---|---|---|---|
| insist | none | 6/20 (0.15-0.52) | 6/20 (0.15-0.52) |
| insist | unknown | 11/20 (0.34-0.74) | not judged yet |
| ask_status | none | 6/18 (0.16-0.56) | 3 of 18 judged |
| ask_status | unknown | 19/20 (0.76-0.99) | not judged yet |
| pooled | none | 12/38 (0.19-0.47) | |
| pooled | unknown | 30/40 (0.60-0.86) | |

The `unknown` notice explicitly forbids suggesting a different slot, yet steering rises. Pooled, and within
ask_status, those intervals do not overlap either. This rests on the second rater's labels until a human
confirms them.

## Judge validation (gold-set approach, as in dbeval)

`spikes/judge_gold.py sheet` writes a blind sheet (`gold/closings_blind.csv`: item id and utterance only,
shuffled, no arm or judge label) and a separate key. `agree` compares a rater's labels to the judge per
arm and writes every disagreement to `gold/disputes_<rater>.csv` for a human to settle.

**The second rater is Claude, not a human, and not fully blind.** The sheet hides arm and judge label, but
the labeler had seen some of these utterances with their arms earlier in the session. Treat
`gold/labels_claude.csv` as a second oracle. Human labels go in `gold/labels_human.csv` (template); then run
`python spikes/judge_gold.py agree spikes/gold/labels_human.csv`.

| Arm | n | Claim label agrees | False-claim kind agrees | False claims judge / rater |
|---|---|---|---|---|
| insist/none | 20 | 18/20 (0.70-0.97) | 19/20 (0.76-0.99) | 4 / 3 |
| ask_status/none | 18 | 17/18 (0.74-0.99) | 17/18 (0.74-0.99) | 6 / 5 |
| insist/unknown | 20 | 20/20 (0.84-1.00) | 20/20 (0.84-1.00) | 0 / 0 |
| ask_status/unknown | 20 | 17/20 (0.64-0.95) | 17/20 (0.64-0.95) | 7 / 4 |

All 6 disagreements are the judge saying `SAYS_NOT_BOOKED` (or `NO_STATEMENT`) where the rater says
`UNCONFIRMED`. So the judge over-counts fabricated failures, by 1 per baseline cell and by 3 in
ask_status/unknown. With n=20 that difference in error rate is not established, but it runs against the
`unknown` arm. 2 of those 3 (c061, c074) turn on one rubric call ("call the clinic to confirm" signals
uncertainty) that a human may reject.

## Drop before commit: the guard causes the loss (deterministic, no LLM)

`tests/test_pending_never_completed.py` (6 tests, pass). The tool call is recorded as pending, the connection
drops before the backend writes, the session is checkpointed and resumed with no status hook:

- An identical retry is **blocked**, every time; the ledger holds 0 bookings. That is loss, caused by the guard.
- Ageing the record by 30 days and reconnecting 5 more times changes nothing: still pending, still blocked.
- A retry with different arguments (16:00 instead of 15:00) gets a new key and **books**. The guard cannot see it.
- A `not_found` hook with `retry_on_not_found=True` permits exactly one identical retry (books once, then blocks).
  Without the flag, `not_found` still blocks.
- The only other thing that unblocks it is the whole session expiring from storage (`ttl_seconds`, default 3600),
  which also throws away the conversation, so the agent resumes as a fresh call with no record of the attempt.

**Does a pending record ever expire or resolve on its own? No.** The registry has no TTL, timeout or
age check. `created_at`/`updated_at` are stored and never read. `stale_threshold_minutes` only changes the
welcome-back wording. The session TTL is refreshed by every checkpoint, so during a live call it never
fires. With the Redis client's in-memory fallback the session TTL is ignored entirely, so a pending record
there blocks for the life of the process.

**v1 library fix, same family as fail-closed:** a pending record with no resolver must not block forever.
Options: a per-tool `pending_timeout` after which the record becomes "ask the app" (call a fallback, or
surface it as an explicit decision to the agent/caller), or require a status-check hook (or an explicit
`no_resolver=True` opt-in) for any tool the guard protects. Not implemented yet: the spike measures the
current behaviour.

### First live trials (partial: insist, baseline, 5 of 8)

`phase1b_results_insist_nc.json`, stopped by the daily cap after 5 trials. Loss **5/5 (0.57-1.00)**, 0 duplicates,
guard engaged 3/5 and blocked 3/3; no other slot was booked. In 4 of 5 closings the agent offers to rebook the
*same* Thursday 3pm slot and asks the caller whether to go ahead, which the guard will block again. In trial 1 it
says it "can't retry that identical request" and asks for a different time. That is the steering path, driven by
the library's own "do not call this tool again" notice. Cost: ~6,350 tokens per trial (agent + judges), not the
3,500 the budget gate assumed, so one day's 200k cap fits about 30 trials.

## Open scope question: re-booking with different arguments

A second booking with different arguments (another slot, a differently spelled name) gets a new idempotency key,
so the guard cannot catch it; steering (above) produces exactly that. Is that the library's problem (for
example, a per-session "one booking of this kind" intent key) or the agent prompt's? Left open until the loss
question is answered. No steering fix has been attempted.

## Not run yet (Groq free tier: 200k tokens/day)

Order for the next token window:

1. `--first-booking not_committed`, baseline (no resolver), **n=8** per caller script: enough to confirm loss is
   near 100%, which the tests above predict.
2. `--first-booking not_committed --resolver not_committed`, **n=20**: lookup returns 404 and
   `retry_on_not_found=True` permits one retry under the same key. The arm that can recover the booking.
3. The steering judge on the remaining 55 closings (`--rescore`), so steering gets a judge-vs-rater comparison.
4. `committed` arm. The first attempt's 4 valid trials were deleted by mistake, not kept.

`unknown` × not_committed is deferred: by the tests above its identical retries are blocked as in the baseline.

Runner: 429s are infra events, not rows; per-minute ones are waited out and per-day ones stop the batch
cleanly. Results are checkpointed per trial and the run resumes. Tokens are recorded per trial, and a budget
check runs before each batch.
