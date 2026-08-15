# Contributing to pipecat-session-continuity

First off, thank you for considering contributing to `pipecat-session-continuity`! It's people like you that make open source such a great community.

## Where to Start
- **Bug reports**: Open an issue if you encounter a bug.
- **Feature ideas**: We'd love to hear them! Open a Discussion or Issue to propose new features.
- **Code contributions**: See the development setup below.

We are especially looking for help with:
- Improving tool-call idempotency
- Adding more storage backends (e.g., SQLite)
- Production battle-testing & edge cases
- Documentation & examples

## Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/meet987654/pipecat-session-continuity.git
   cd pipecat-session-continuity
   ```

2. **Set up a virtual environment** (recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
   ```

3. **Install dependencies** (e.g. using `uv` or `pip`):
   ```bash
   pip install -r requirements.txt
   ```
   *If you are using `uv`, you can just run `uv sync`.*

4. **Run tests**:
   Ensure you have a Redis instance running locally (e.g. `docker-compose up -d`), then run:
   ```bash
   pytest
   ```

## Pull Request Process

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code follows standard PEP 8 guidelines.
6. Open a PR with a clear title and description against the `main` branch.

## Code of Conduct
By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).
