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

## Not run yet (Groq free tier: 200k tokens/day)

- **`--first-booking not_committed`**, baseline and `unknown`, both caller scripts. Implemented: the server
  drops the call before writing, and `loss` = the requested Thursday 15:00 slot never booked.
  **Prediction from the code:** loss near 20/20 in *both* arms. The checkpointed record is pending with the
  same arguments, so the guard blocks any identical retry whether or not the agent tries; only a booking
  with different arguments (another slot, or a differently spelled name) gets through. A difference between
  the arms would then come from steering, not from retrying. `--resolver not_committed` (lookup 404, then
  `retry_on_not_found=True` permits one retry under the same key) is the arm that can recover the booking.
- The steering judge on the remaining 55 closings (`--rescore` resumes).
- `committed` arm. The first attempt's 4 valid trials were deleted by mistake, not kept.

Runner: 429s are infra events, not rows; per-minute ones are waited out and per-day ones stop the batch
cleanly. Results are checkpointed per trial and the run resumes. Tokens are recorded per trial, and a budget
check runs before each batch.
