# Polymarket BTC Bot

This project does one thing: for each newly starting Polymarket BTC five-minute
market, it rests equal post-only buy orders on Up and Down. When either entry
fills, it waits for the conditional tokens to settle and then places the
configured take-profit ladder for the available matched shares. Any fraction
not assigned to the ladder is held through resolution. It runs continuously
until stopped.

## Strategy

- BTC Up or Down five-minute markets only.
- Place both GTC orders before the market starts.
- Buy price and quote amount are required `run` arguments.
- `--take-profit PRICE:FRACTION` is repeatable. Fractions not assigned to an
  exit rung are held through resolution. Omitting every rung selects buy-only
  mode.
- Do not chase prices or cancel one side during the market.
- With take-profit rungs, `MATCHED` records the fill but does not make the
  position sellable. The bot waits for a `MINED`/`CONFIRMED` trade update or an
  available conditional-token balance before offering each configured fraction.
- Cancel any unfilled remainder after the market ends.
- Runtime, open reservation, and daily filled-cost limits are optional CLI inputs.
- An omitted limit is unlimited.
- Cancel remaining entry and exit orders before the market cutoff; the lead time
  is controlled by `--cancel-before-end-seconds` and defaults to two seconds.
  Set it to `0` to leave orders resting until Polymarket closes the market.
- Polymarket's disconnect-cancels-orders heartbeat is disabled unless
  `--heartbeat-seconds SECONDS` is supplied.
- Every 30 minutes, redeem resolved winning positions back into available pUSD.

This is an experimental tail-event strategy. It has not been proven to have a
stable positive expectation. Use a dedicated wallet and small limits.

## HTTP stack

Authenticated CLOB traffic uses `py-clob-client-v2 -> httpx -> httpcore`.
`httpcore` is pinned to commit
`35ddb373e13be5940e5137798d5a63d67e10f3e2` from
`baizhufbb/httpcore`, which contains the proxy TLS zombie-connection fix.
Public Gamma discovery continues to use `requests`. In live farthest-first mode,
Gamma exposes the next deterministic market slug and token IDs before trading
opens. A background probe watches that exact CLOB market every 250 milliseconds
and submits as soon as it accepts orders and both books exist. Gamma fills the
existing window once at startup; ongoing new entries use only the CLOB activation
probe.

## Configuration

Wallet credentials live in `.env.trading`. Trading values and optional limits
are supplied on the command line.

Trading values are fixed when the process starts. For example, the following
ladder buys 100 shares per filled side at one cent, offers 50 shares at two
cents, 10 at ten cents, 10 at thirty cents, and holds the remaining 30 shares:

```powershell
--buy-price 0.01 --usd-per-side 1 `
  --take-profit 0.02:0.50 `
  --take-profit 0.10:0.10 `
  --take-profit 0.30:0.10
```

Order size is `usd-per-side / buy-price`. Each take-profit fraction is applied
to the matched entry size, including incremental partial fills.

Omitting `--take-profit` selects buy-only mode. Matched shares are not offered
for sale; resolved winning positions are collected by the 30-minute redemption
worker.

Omitting `--heartbeat-seconds` leaves GTC orders on the exchange during a
network or process outage. Supplying a value above zero and below ten enables
Polymarket's dead-man switch and controls how often this client sends a
heartbeat. It does not change Polymarket's server-side cancellation deadline.

## Safety

- `run` is a dry-run unless `--live` is supplied.
- Live mode also requires `POLYMARKET_LIVE_ACK=I_UNDERSTAND_REAL_ORDERS`.
- Order submission retries are disabled. An ambiguous response is recorded and
  is not blindly submitted again.
- Up and Down are submitted in one batch. If only one is accepted, the bot
  immediately cancels it.
- Entry and take-profit orders are ordinary GTC limits, so they do not depend on a
  minimum GTD lifetime. Entries are post-only. A marketable exit target can
  execute at the target price or better. Only the matched entry quantity is
  offered.
- Uncovered fills below the market's minimum order size are accumulated until
  enough shares are available for a valid exit order.
- Exit submission is also capped by the exchange-reported conditional-token
  balance, so off-chain matches are never treated as already settled inventory.
- An ambiguous exit submission is reconciled against the exchange. If it cannot
  be identified exactly, it is recorded as failed and is not blindly repeated.
- When `--heartbeat-seconds` is enabled, a dedicated thread sends independently
  of discovery and reconciliation. A failed heartbeat pauses new orders and
  Polymarket may cancel every open order for the account.
- Farthest-first live runs record CLOB acceptance detection, both-book readiness,
  order-submission duration, and the aggregate queue already resting at the
  configured entry price.
- A separate redemption thread scans every 30 minutes. It redeems only resolved
  positions with positive current value, waits for relayer confirmation, and
  never blindly retries an ambiguous or failed redemption in the same process.
- When enabled, an expired heartbeat ID is replaced from the protocol's `400`
  response and retried once. Recovery triggers an immediate open-order
  reconciliation.
- Open orders are synchronized in one batch; only orders missing from that
  response require an individual terminal-status lookup. Exchange reads run in
  a background worker so reconciliation cannot delay a new-market placement.
- Ctrl+C cancels all open orders recorded by this bot. Every other shutdown path,
  including runtime failures and a completed `--hours` duration, leaves exchange
  orders open for the next run to reconcile.
- Entry placement is attempted only once per market across all runs. A cancelled,
  rejected, failed, or ambiguous entry pair is never submitted again because a
  later order would lose the original queue position.
- The bot checks Polymarket geoblocking before and during live operation.
- A local process lock prevents two bot instances from placing duplicate orders.
- Runtime geoblock checks run outside the main loop. A temporary network failure
  pauses new placements and retries every five seconds without stopping order
  tracking; an explicit blocked response still stops the run.

## Database

The only database is `data/bot.sqlite`:

- `runs`: every start and stop, mode, fixed trading parameters, optional
  heartbeat status, and terminal error.
- `markets`: each market considered by the bot and its state.
- `orders`: entry and exit order IDs, side, price, size, matched size, and status.
- `events`: operational audit log.

There is no market-data warehouse and no historical backtest database.

## Commands

```powershell
# Install pinned dependencies.
uv sync --dev

# After setting POLYMARKET_PRIVATE_KEY, show the derived signer address only.
uv run --env-file .env.trading bot.py setup

# After signing in with the robot signer wallet, add its Relayer API key and
# matching owner address from Settings > API Keys, then deploy the signer's default
# Deposit Wallet, set missing trading approvals, and save the derived funder
# address and CLOB credentials. This does not deposit funds or place orders.
uv run --env-file .env.trading bot.py setup --apply

# Check authentication, location, collateral, allowance, and open orders.
uv run --env-file .env.trading bot.py doctor

# Run a continuous dry-run. No credentials or orders are used.
uv run bot.py run --buy-price 0.01 --usd-per-side 1 `
  --take-profit 0.02:0.50 `
  --take-profit 0.10:0.10 `
  --take-profit 0.30:0.10

# Run a continuous buy-only dry-run.
uv run bot.py run --buy-price 0.02 --usd-per-side 1 --lookahead-minutes 40

# Show database state.
uv run bot.py status

# Run live continuously.
uv run --env-file .env.trading bot.py run --live `
  --buy-price 0.01 --usd-per-side 1 `
  --take-profit 0.02:0.50 `
  --take-profit 0.10:0.10 `
  --take-profit 0.30:0.10

# Opt into disconnect-triggered order cancellation with a five-second heartbeat.
uv run --env-file .env.trading bot.py run --live `
  --buy-price 0.01 --usd-per-side 1 `
  --heartbeat-seconds 5

# Select 40 minutes from the currently farthest market and place backward.
uv run --env-file .env.trading bot.py run --live `
  --buy-price 0.01 --usd-per-side 1 `
  --take-profit 0.02:0.50 `
  --take-profit 0.10:0.10 `
  --take-profit 0.30:0.10 `
  --lookahead-minutes 40 --placement-order farthest-first `
  --cancel-before-end-seconds 2

# Run with explicit optional limits.
uv run --env-file .env.trading bot.py run --live `
  --buy-price 0.01 --usd-per-side 1 `
  --take-profit 0.02:0.50 `
  --take-profit 0.10:0.10 `
  --take-profit 0.30:0.10 `
  --hours 24 --max-reserved-usd 10 --max-daily-filled-cost 20

# Tests.
uv run pytest
```

The bot records matched entry and exit sizes and redeems resolved winning
positions through the configured Relayer credentials. A successful exit sale
already returns pUSD immediately and therefore needs no redemption. Make sure
the wallet has enough available pUSD for the orders you allow the bot to open.
