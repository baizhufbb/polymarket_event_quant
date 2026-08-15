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
- Live entries wait for the earliest public evidence that the book exists —
  a REST books poll and the market WebSocket feed race each other — then
  submit the pre-signed batch in a burst. Probing the order endpoint while
  the book is closed only burns the rate-limit budget. `--placement-interval-ms`
  sets the burst cadence and defaults to 40 milliseconds, roughly the
  sustained rate the order endpoint accepts without rejections.
- The bot does not submit redemption transactions. Resolved winnings are
  returned by Polymarket's account-side settlement service.

This is an experimental tail-event strategy. It has not been proven to have a
stable positive expectation. Use a dedicated wallet and small limits.

## HTTP stack

Authenticated CLOB traffic uses `py-clob-client-v2 -> httpx -> httpcore`.
`httpcore` is pinned to commit
`35ddb373e13be5940e5137798d5a63d67e10f3e2` from
`baizhufbb/httpcore`, which contains the proxy TLS zombie-connection fix.
Public discovery continues to use `requests`. Gamma fills the existing window
once, then the predictable next five-minute slug is queried every second. These
requests include a unique cache buster because Gamma otherwise advertises a
five-minute public cache. Each newly discovered condition is checked against
`GET /clob-markets/{condition_id}` every 250 milliseconds. As soon as CLOB
returns both real token IDs, the main loop is woken; the service then waits
for the market's first public order-book event and submits the post-only pair
in a burst.

## Runtime sequence and interfaces

The scheduled five-minute slot, Gamma metadata creation time, CLOB parameter
availability, and order acceptance time are separate. Markets may become ready
out of slot order, so each condition is tracked independently.

| Stage | Interface | Code |
| --- | --- | --- |
| Parse command and validate credentials | Local CLI and `.env.trading` | `polymarket_bot/cli.py`, `polymarket_bot/config.py` |
| Open the run and local state | SQLite `data/bot.sqlite` | `polymarket_bot/database.py` |
| Discover startup metadata | `GET https://gamma-api.polymarket.com/events` | `polymarket_bot/discovery.py` |
| Discover every new market | Cache-busted `GET https://gamma-api.polymarket.com/markets/slug/{slug}` every second | `polymarket_bot/discovery.py`, `polymarket_bot/market_activation.py` |
| Detect CLOB market parameters | `GET https://clob.polymarket.com/clob-markets/{condition_id}` every 250 ms | `polymarket_bot/market_activation.py` |
| Submit both entry orders | Authenticated CLOB batch `post_orders`, post-only GTC | `polymarket_bot/exchange.py` |
| Receive fills and cancellations | Authenticated CLOB user WebSocket | `polymarket_bot/user_stream.py` |
| Verify open-order state | CLOB `get_open_orders` and `get_order` | `polymarket_bot/reconciliation.py` |
| Place configured exits | CLOB conditional-token balance plus GTC sell orders | `polymarket_bot/service.py`, `polymarket_bot/exchange.py` |

The live farthest-first path runs in this order:

1. Gamma fills the configured far-edge window once. The worker then predicts the
   exact next BTC five-minute slug and requests that slug every second with a
   unique cache buster until its condition and real Up/Down token IDs exist.
2. Each discovered condition is checked independently through
   `/clob-markets/{condition_id}` every 250 milliseconds.
3. When CLOB returns both token IDs, only that market leaves the pending set.
   The service validates eligibility, database uniqueness, tick size, minimum
   size, and optional limits.
4. The exchange signs Up and Down locally and submits both in one authenticated
   post-only batch. An explicit engine-readiness rejection (`invalid token id`,
   `market not found`, market not ready, or missing order books) preserves the
   signed pair and immediately retries from the main loop without another
   parameter request or an added delay. Ambiguous network failures are
   reconciled before retrying. If that read is temporarily unavailable, the
   same signed pair remains pending and is reconciled again on the next attempt.
   If exactly one side is accepted, it is canceled.
5. Accepted market and order IDs are committed to SQLite. The user WebSocket
   and REST reconciliation then maintain fills and terminal states.
6. In buy-only mode, matched shares are held through resolution. With
   `--take-profit`, settled matched inventory is offered at the configured
   exit rungs. Redemption is outside the trading process.

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
for sale; they remain held through resolution.

Omitting `--heartbeat-seconds` leaves GTC orders on the exchange during a
network or process outage. Supplying a value above zero and below ten enables
Polymarket's dead-man switch and controls how often this client sends a
heartbeat. It does not change Polymarket's server-side cancellation deadline.

## Safety

- `run` is a dry-run unless `--live` is supplied.
- The ordinary dry-run checks discovery and order planning only. It does not
  model fills or profitability. Use `paper` for an execution-aware simulation.
- Live mode also requires `POLYMARKET_LIVE_ACK=I_UNDERSTAND_REAL_ORDERS`.
- During activation, the bot holds fire until the market's first public book
  event, then submits the same signed Up/Down batch at the
  `--placement-interval-ms` cadence until both orders are accepted. Each cadence
  tick starts its submission immediately. Overlapping requests reuse the
  original order hashes, so a duplicate response
  identifies the original order instead of creating another position. If the
  result remains ambiguous, the bot reconciles exchange orders. A temporary
  reconciliation outage keeps the market pending instead of permanently
  dropping it.
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
- Farthest-first live runs record Gamma discovery, CLOB parameter detection, and
  order-submission timestamps.
- When enabled, an expired heartbeat ID is replaced from the protocol's `400`
  response and retried once. Recovery triggers an immediate open-order
  reconciliation.
- Open orders are synchronized in one batch; only orders missing from that
  response require an individual terminal-status lookup. Exchange reads run in
  a background worker so reconciliation cannot delay a new-market placement.
- Ctrl+C cancels all open orders recorded by this bot. Every other shutdown path,
  including runtime failures and a completed `--hours` duration, leaves exchange
  orders open for the next run to reconcile.
- Explicitly rejected or partially accepted entry batches are not retried.
  Engine-not-ready, confirmed-empty ambiguous submissions, and temporary
  submission/reconciliation outages remain eligible until a complete Up/Down
  pair is accepted or the market ends.
- The bot checks Polymarket geoblocking before and during live operation.
- A local process lock prevents two bot instances from placing duplicate orders.
- Runtime geoblock checks run outside the main loop. A temporary network failure
  pauses new placements and retries every five seconds without stopping order
  tracking; an explicit blocked response still stops the run.

## Database

Live and ordinary dry-run state use `data/bot.sqlite`:

- `runs`: every start and stop, mode, fixed trading parameters, optional
  heartbeat status, and terminal error.
- `markets`: each market considered by the bot and its state.
- `orders`: entry and exit order IDs, side, price, size, matched size, and status.
- `events`: operational audit log.

There is no market-data warehouse and no historical backtest database.

Paper simulation uses the separate `data/paper.sqlite`. It records the public
0.01 queue when each market first becomes observable, then settles the
hypothetical orders from public taker trades after the market resolves. The
fill model is FIFO and deliberately does not assume that orders ahead canceled,
so exact-price fills are a conservative lower bound. A trade through the limit
price confirms a full fill. Direct sells of an outcome and complementary buys
of the opposite outcome are both counted. Reported PnL excludes variable maker
rebates and never uses wallet credentials or authenticated order endpoints.
The status report also shows active and peak order reserve. Paper mode assumes
every observed market receives both configured orders even when that requires
more collateral than the account actually holds; use the reserve figures when
judging whether its PnL is practically reproducible.

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

# Simulate the current one-cent, $1-per-side hold-to-resolution strategy.
# With lookahead 0, existing markets are skipped and the first settled samples
# arrive only after newly announced markets have finished.
uv run bot.py paper --buy-price 0.01 --usd-per-side 1 `
  --lookahead-minutes 0

# Show fill counts, conservative PnL, ROI, and recent paper markets.
uv run bot.py paper-status

# Run live continuously.
uv run --env-file .env.trading bot.py run --live `
  --buy-price 0.01 --usd-per-side 1 `
  --placement-interval-ms 20 `
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

# Skip existing markets and begin with the next newly announced farthest market.
uv run --env-file .env.trading bot.py run --live `
  --buy-price 0.01 --usd-per-side 1 `
  --lookahead-minutes 0 --placement-order farthest-first `
  --cancel-before-end-seconds 0

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

The bot records matched entry and exit sizes but does not redeem resolved
positions. A successful exit sale returns pUSD immediately; resolved winnings
are handled outside this process. Make sure the wallet has enough available
pUSD for the orders you allow the bot to open.
