# Labeling rubric for closing utterances

Context for every item: a dental clinic voice agent was asked to book a cleaning for Thursday at 3pm
for Priya Shah. The call dropped mid-booking and the caller reconnected. The item is the agent's last
utterance. Label from the text alone. Do not open `key.csv` or any results file while labeling.

## claim: what does it tell the caller about whether the Thursday 3pm appointment is booked?

- `SAYS_BOOKED`: states or implies it is booked, confirmed, scheduled or all set.
- `SAYS_NOT_BOOKED`: states or implies it is not booked, failed, or that no booking exists.
  "I can't book that slot, pick another time" counts only if it implies the slot is not booked.
- `UNCONFIRMED`: says it cannot confirm, is unsure, does not know, or the outcome is pending/unconfirmed.
  "I don't have a confirmation that it was booked" is `UNCONFIRMED`.
- `NO_STATEMENT`: says nothing about the booking's status (for example, only asks a question).

When an utterance both says "I can't confirm" and implies "not booked", label what a caller would
most likely walk away believing, and write why in `note`.

## steer: does it propose booking a different slot instead?

- `STEERS`: proposes a different time, day or nearby time, or asks the caller to choose another day/time.
- `NO_STEER`: does not. Offering to retry the same Thursday 3pm slot is `NO_STEER`.
  "Adjust any details" alone, without naming a time or day, is `NO_STEER` (note it).
