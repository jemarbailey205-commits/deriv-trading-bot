"""
Deriv AO + AC Breakout Bot  (API v1)
-------------------------------------------------------
Strategy (run independently per symbol), 15-minute candles:

Step 1 - Momentum (either trigger):
  Trigger 1: AO and AC BOTH cross the zero line in the same direction,
             within CROSS_GAP candles of each other.
  Trigger 2: ONE of AO/AC crosses the zero line, and the OTHER shows a
             "loose saucer" in the same direction (measurable, see below).
             The other indicator does not need to cross zero.

Step 2 - Structure: the signal candle (latest closed candle) must close
  beyond the swing high (bullish) or swing low (bearish) of the prior
  STRUCTURE_LOOKBACK candles.

Step 3 - Trade in the confirmed direction.

Authentication (Deriv API v1):
- GET accounts -> find demo account ID
- POST OTP only when a signal fires, then connect and trade immediately

Environment variables (Railway -> Variables):
  DERIV_API_TOKEN, DERIV_APP_ID
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

import aiohttp
import websockets
import pandas as pd
import ta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

# ====== CONFIG ======
API_TOKEN = os.environ.get("DERIV_API_TOKEN")
APP_ID = os.environ.get("DERIV_APP_ID")

SYMBOLS = ["R_25", "R_75", "R_100"]
GRANULARITY = 900
CANDLE_COUNT = 110

# Dynamic stake sizing (USD), per individual trade
BALANCE_TIER_USD = 50.0
STAKE_PERCENT_BELOW = 0.20
STAKE_PERCENT_AT_ABOVE = 0.60
MIN_STAKE_USD = 0.35

RISK_STATE_FILE = os.environ.get(
    "RISK_STATE_FILE", "bot_risk_state.json"
)

CONSECUTIVE_LOSS_LIMIT = 2
LOSS_PAUSE_HOURS = 3

DURATION = 5
DURATION_UNIT = "m"

# --- AO / AC settings ---
AC_SMA_PERIOD = 5
MIN_CANDLES = 50
ATR_PERIOD = 14

# --- Momentum triggers ---
TRIGGER1_ENABLED = True
TRIGGER2_ENABLED = True
CROSS_GAP = 3
MOMENTUM_MAX_AGE = 3
SAUCER_BARS = 2
SAUCER_MIN_ATR = 0.05

# --- Structure filter ---
STRUCTURE_LOOKBACK = 25
BREAKOUT_WINDOW = 1

# --- Optional breakout filters ---
BREAKOUT_BUFFER_ENABLED = False
BREAKOUT_BUFFER_ATR = 0.25
BODY_FILTER_ENABLED = False
MIN_BODY_RATIO = 0.6

# --- Duplicate protection ---
COOLDOWN_CANDLES = 4

# --- Rest period: Jamaica time ---
REST_ENABLED = True
REST_TIMEZONE = "America/Jamaica"
REST_START_DAY = 4
REST_START_HOUR = 18
REST_END_DAY = 5
REST_END_HOUR = 18

# --- Timing / robustness ---
CANDLE_CLOSE_DELAY = 2
WS_TIMEOUT = 15
BUY_RETRIES = 2
BUY_RETRY_DELAY = 1

_trade_lock = None

_risk_state = {
    "consecutive_losses": 0,
    "paused_until": 0.0,
}

ACCOUNTS_ENDPOINT = (
    "https://api.derivws.com/trading/v1/options/accounts"
)
PUBLIC_WS_URL = (
    "wss://api.derivws.com/trading/v1/options/ws/public"
)


def get_headers():
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": str(APP_ID),
        "Content-Type": "application/json",
    }


# ====== REST PERIOD FILTER ======

def _local_now() -> datetime:
    """Return current Jamaica time."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(REST_TIMEZONE))
        except Exception:
            pass

    return datetime.now(timezone(timedelta(hours=-5)))


def is_rest_period() -> bool:
    """Check the Friday 6 PM to Saturday 6 PM rest window."""
    if not REST_ENABLED:
        return False

    now = _local_now()

    now_min = now.weekday() * 1440 + now.hour * 60 + now.minute
    start_min = REST_START_DAY * 1440 + REST_START_HOUR * 60
    end_min = REST_END_DAY * 1440 + REST_END_HOUR * 60

    if start_min <= end_min:
        return start_min <= now_min < end_min

    return now_min >= start_min or now_min < end_min


# ====== ACCOUNT DISCOVERY ======

async def get_demo_account_id() -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            ACCOUNTS_ENDPOINT,
            headers=get_headers()
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"Accounts request failed ({resp.status}): {body}"
                )

            data = await resp.json()
            accounts = data.get("data", [])

            if not accounts:
                raise RuntimeError(
                    "No Options accounts found. Make sure your PAT has "
                    "the 'trade' scope and belongs to the same Deriv login "
                    "as your demo Options account."
                )

            for acc in accounts:
                if (
                    acc.get("account_type") == "demo"
                    and acc.get("status") == "active"
                ):
                    account_id = acc["account_id"]
                    print(
                        f"Found demo Options account: {account_id} "
                        f"(balance: {acc['balance']} {acc['currency']})"
                    )
                    return account_id

            print("Available accounts:")
            for acc in accounts:
                print(
                    f"  {acc.get('account_id')} | "
                    f"type: {acc.get('account_type')} | "
                    f"status: {acc.get('status')}"
                )

            raise RuntimeError("No active demo Options account found.")async def get_fresh_otp_url(account_id: str) -> str:
    otp_endpoint = (
        f"https://api.derivws.com/trading/v1/options/accounts/"
        f"{account_id}/otp"
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(
            otp_endpoint, headers=get_headers()
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"OTP request failed ({resp.status}): {body}"
                )

            data = await resp.json()
            print("Fresh OTP URL obtained.")
            return data["data"]["url"]


# ====== RISK MANAGEMENT ======

def _save_risk_state() -> None:
    """Persist circuit-breaker state across restarts."""
    try:
        Path(RISK_STATE_FILE).write_text(
            json.dumps(_risk_state, indent=2)
        )
    except Exception as exc:
        print(f"WARNING: Could not persist risk state: {exc}")


def _load_risk_state() -> None:
    try:
        path = Path(RISK_STATE_FILE)

        if path.exists():
            loaded = json.loads(path.read_text())

            _risk_state["consecutive_losses"] = int(
                loaded.get("consecutive_losses", 0)
            )
            _risk_state["paused_until"] = float(
                loaded.get("paused_until", 0.0)
            )

    except Exception as exc:
        print(f"WARNING: Could not load risk state: {exc}")


def circuit_breaker_active() -> bool:
    paused_until = float(_risk_state.get("paused_until", 0.0))

    if paused_until <= time.time():
        if paused_until:
            _risk_state["paused_until"] = 0.0
            _save_risk_state()
        return False

    remaining = int(paused_until - time.time())

    print(
        f"LOSS CIRCUIT BREAKER ACTIVE: "
        f"{remaining // 60}m {remaining % 60}s remaining."
    )
    return True


def record_confirmed_result(
    profit: float, symbol: str, contract_id
) -> None:
    """Update the global loss streak using confirmed results only."""

    if profit < 0:
        _risk_state["consecutive_losses"] = (
            int(_risk_state.get("consecutive_losses", 0)) + 1
        )

        print(
            f"[{symbol}] Confirmed LOSS on contract {contract_id}: "
            f"{profit:.2f}. Consecutive losses: "
            f"{_risk_state['consecutive_losses']}."
        )

        if (
            _risk_state["consecutive_losses"]
            >= CONSECUTIVE_LOSS_LIMIT
        ):
            _risk_state["paused_until"] = (
                time.time() + LOSS_PAUSE_HOURS * 3600
            )

            print(
                f"LOSS CIRCUIT BREAKER: "
                f"{CONSECUTIVE_LOSS_LIMIT} consecutive losses. "
                f"Trading paused for {LOSS_PAUSE_HOURS} hours."
            )

    elif profit > 0:
        _risk_state["consecutive_losses"] = 0

        print(
            f"[{symbol}] Confirmed WIN on contract {contract_id}: "
            f"+{profit:.2f}. Consecutive-loss counter reset."
        )

    else:
        print(
            f"[{symbol}] Contract {contract_id} closed at break-even; "
            "consecutive-loss counter unchanged."
        )

    _save_risk_state()


async def get_account_balance(
    account_id: str
) -> tuple[float, str]:
    """Fetch the current account balance."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            ACCOUNTS_ENDPOINT, headers=get_headers()
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"Balance request failed ({resp.status}): {body}"
                )

            data = await resp.json()

    for acc in data.get("data", []):
        if str(acc.get("account_id")) == str(account_id):
            return (
                float(acc["balance"]),
                str(acc.get("currency", "")),
            )

    raise RuntimeError(
        f"Selected account {account_id} was not returned."
    )


def calculate_stake(balance: float, currency: str) -> float:
    """20% below $50; 60% at/above $50, per trade."""

    if currency.upper() != "USD":
        raise RuntimeError(
            f"Stake tiers are defined in USD, but account currency "
            f"is {currency!r}. Refusing to trade."
        )

    percentage = (
        STAKE_PERCENT_BELOW
        if balance < BALANCE_TIER_USD
        else STAKE_PERCENT_AT_ABOVE
    )

    stake = round(balance * percentage, 2)

    if stake < MIN_STAKE_USD:
        raise RuntimeError(
            f"Calculated stake ${stake:.2f} is below the minimum "
            f"${MIN_STAKE_USD:.2f}; trade skipped."
        )

    print(
        f"Balance ${balance:.2f}; stake tier {percentage:.0%}; "
        f"per-trade stake ${stake:.2f}."
    )

    return stake


# ====== CANDLES ======

async def get_closed_candles(symbol: str) -> list:
    request = {
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": CANDLE_COUNT,
        "end": "latest",
        "granularity": GRANULARITY,
        "style": "candles",
    }

    async with websockets.connect(PUBLIC_WS_URL) as ws:
        await ws.send(json.dumps(request))

        while True:
            response = json.loads(
                await asyncio.wait_for(
                    ws.recv(), timeout=WS_TIMEOUT
                )
            )

            if "error" in response:
                raise RuntimeError(
                    response["error"].get(
                        "message", "Unknown Deriv error"
                    )
                )

            if response.get("msg_type") == "candles":
                candles = response["candles"]
                last = candles[-1]

                forming = (
                    "epoch" not in last
                    or last["epoch"] + GRANULARITY > time.time()
                )

                closed = candles[:-1] if forming else candles

                print(
                    f"[{symbol}] Fetched {len(candles)} candles, "
                    f"using {len(closed)} closed."
                )

                return closed


def build_dataframe(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)

    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)

    return df


def candle_id(df: pd.DataFrame, idx: int) -> int:
    """Return the candle epoch, or its position if unavailable."""
    if "epoch" in df.columns:
        return int(df["epoch"].iloc[idx])

    return len(df) + idx


# ====== MOMENTUM HELPERS ======

def last_cross(
    series: pd.Series, direction: str, max_back: int
):
    """Find the latest zero-line cross in the requested direction."""

    last = series.iloc[-1]

    if direction == "up" and not last > 0:
        return None

    if direction == "down" and not last < 0:
        return None

    for k in range(max_back + 1):
        idx = -1 - k
        prev, cur = series.iloc[idx - 1], series.iloc[idx]

        if direction == "up" and prev <= 0 < cur:
            return k

        if direction == "down" and prev >= 0 > cur:
            return k

    return None


def has_saucer(
    series: pd.Series, direction: str, atr: float
) -> bool:
    """Detect a loose saucer using indicator-height changes."""

    n = SAUCER_BARS
    diffs = series.diff()
    recent = diffs.iloc[-n:]
    before = diffs.iloc[-(n + 1)]
    min_move = SAUCER_MIN_ATR * atr

    if direction == "up":
        if not ((recent > 0).all() and before < 0):
            return False

        return (
            series.iloc[-1] - series.iloc[-(n + 1)]
        ) >= min_move

    else:
        if not ((recent < 0).all() and before > 0):
            return False

        return (
            series.iloc[-(n + 1)] - series.iloc[-1]
        ) >= min_movedef detect_momentum(df, ao, ac, direction: str, atr: float):
    """Return the trigger name and crossover event epoch, if valid."""

    lookback = MOMENTUM_MAX_AGE + CROSS_GAP

    k_ao = last_cross(ao, direction, lookback)
    k_ac = last_cross(ac, direction, lookback)

    # Trigger 1: both indicators cross zero within CROSS_GAP candles.
    if (
        TRIGGER1_ENABLED
        and k_ao is not None
        and k_ac is not None
        and abs(k_ao - k_ac) <= CROSS_GAP
        and min(k_ao, k_ac) <= MOMENTUM_MAX_AGE
    ):
        newest = min(k_ao, k_ac)
        return (
            "T1_both_cross",
            candle_id(df, -1 - newest),
        )

    # Trigger 2: one indicator crosses, the other forms a saucer.
    if TRIGGER2_ENABLED:
        for crossed_k, other in ((k_ao, ac), (k_ac, ao)):
            if (
                crossed_k is not None
                and crossed_k <= MOMENTUM_MAX_AGE
                and has_saucer(other, direction, atr)
            ):
                return (
                    "T2_cross_plus_saucer",
                    candle_id(df, -1 - crossed_k),
                )

    return None


# ====== STRUCTURE + FILTERS ======

def broke_level(
    df: pd.DataFrame, direction: str, atr: float
):
    """Check whether price broke the prior swing high or swing low."""

    buffer = (
        BREAKOUT_BUFFER_ATR * atr
        if BREAKOUT_BUFFER_ENABLED
        else 0.0
    )

    close_now = df["close"].iloc[-1]

    for j in range(BREAKOUT_WINDOW + 1):
        window = df.iloc[
            -(STRUCTURE_LOOKBACK + 1 + j):-(1 + j)
        ]

        if len(window) < STRUCTURE_LOOKBACK:
            continue

        close_j = df["close"].iloc[-1 - j]

        if direction == "up":
            level = window["high"].max()

            if (
                close_j > level + buffer
                and close_now > level + buffer
            ):
                return j

        else:
            level = window["low"].min()

            if (
                close_j < level - buffer
                and close_now < level - buffer
            ):
                return j

    return None


def body_is_strong(
    df: pd.DataFrame, direction: str, offset: int = 0
) -> bool:
    """Check the breakout candle body when the filter is enabled."""

    if not BODY_FILTER_ENABLED:
        return True

    candle = df.iloc[-1 - offset]
    candle_range = candle["high"] - candle["low"]

    if candle_range <= 0:
        return False

    body_ratio = (
        abs(candle["close"] - candle["open"]) / candle_range
    )

    if body_ratio < MIN_BODY_RATIO:
        return False

    if direction == "up":
        return candle["close"] > candle["open"]

    return candle["close"] < candle["open"]


# ====== SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    """
    Return signal, candle epoch, trigger and crossover event key.
    Signal is CALL, PUT or None.
    """

    signal_epoch = candle_id(df, -1)

    ao = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"],
        low=df["low"],
    ).awesome_oscillator()

    ac = ao - ao.rolling(AC_SMA_PERIOD).mean()

    atr = ta.volatility.AverageTrueRange(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=ATR_PERIOD,
    ).average_true_range().iloc[-1]

    close_price = df["close"].iloc[-1]

    for direction, signal in (
        ("up", "CALL"),
        ("down", "PUT"),
    ):
        momentum = detect_momentum(
            df, ao, ac, direction, atr
        )

        if momentum is None:
            continue

        trigger, cross_epoch = momentum

        level_name = (
            "resistance" if direction == "up" else "support"
        )

        offset = broke_level(df, direction, atr)

        if offset is None:
            print(
                f"  {trigger} {direction} momentum seen but price "
                f"({close_price:.4f}) hasn't broken {level_name}, "
                "skipping."
            )
            continue

        if offset > 0:
            print(
                f"  {trigger} {direction}: breakout was "
                f"{offset} candle(s) ago and price remains beyond "
                f"{level_name}."
            )

        if not body_is_strong(df, direction, offset):
            print(
                f"  {trigger} {direction} breakout rejected: "
                "weak candle body."
            )
            continue

        return (
            signal,
            signal_epoch,
            trigger,
            (direction, cross_epoch),
        )

    return None, signal_epoch, None, None


# ====== CONTRACT RESULT MONITORING ======

async def monitor_contract(
    account_id: str, symbol: str, contract_id
):
    """Wait for a confirmed contract settlement."""

    otp_url = await get_fresh_otp_url(account_id)

    async with websockets.connect(otp_url) as ws:
        await ws.send(json.dumps({
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1,
        }))

        while True:
            msg = json.loads(
                await asyncio.wait_for(
                    ws.recv(), timeout=WS_TIMEOUT
                )
            )

            if "error" in msg:
                err = msg["error"]
                raise RuntimeError(
                    f"Contract monitoring error: "
                    f"{err.get('message')} | code={err.get('code')}"
                )

            if msg.get("msg_type") == "proposal_open_contract":
                contract = msg.get("proposal_open_contract", {})

                if contract.get("is_sold"):
                    profit = contract.get("profit")

                    if profit is None:
                        print(
                            f"[{symbol}] Contract {contract_id} sold, "
                            "but profit is missing."
                        )
                        return None

                    return float(profit)


# ====== PLACE TRADE ======

async def _attempt_trade(
    account_id: str,
    symbol: str,
    contract_type: str,
    stake: float,
):
    """Request a proposal and attempt one purchase."""

    otp_url = await get_fresh_otp_url(account_id)

    proposal_request = {
        "proposal": 1,
        "amount": stake,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": "USD",
        "duration": DURATION,
        "duration_unit": DURATION_UNIT,
        "underlying_symbol": symbol,
    }

    async with websockets.connect(otp_url) as ws:
        await ws.send(json.dumps(proposal_request))

        while True:
            msg = json.loads(
                await asyncio.wait_for(
                    ws.recv(), timeout=WS_TIMEOUT
                )
            )

            if "error" in msg:
                err = msg["error"]

                print(
                    f"[{symbol}] Proposal error: "
                    f"{err.get('message')} | code={err.get('code')} "
                    f"| details={err.get('details')}"
                )

                return "fail", None

            if msg.get("msg_type") == "proposal":
                proposal = msg.get("proposal", {})
                proposal_id = proposal.get("id")
                ask_price = proposal.get("ask_price")

                if not proposal_id or ask_price is None:
                    print(
                        f"[{symbol}] Proposal response missing "
                        "ID or price."
                    )
                    return "fail", None

                print(
                    f"[{symbol}] Proposal received - ID: {proposal_id}, "
                    f"Ask: {ask_price}, Stake: ${stake:.2f}"
                )
                break

        await ws.send(json.dumps({
            "buy": proposal_id,
            "price": ask_price,
        }))

        try:
            while True:
                buy_response = json.loads(
                    await asyncio.wait_for(
                        ws.recv(), timeout=WS_TIMEOUT
                    )
                )

                if (
                    buy_response.get("msg_type") == "buy"
                    or "error" in buy_response
                ):
                    break

        except asyncio.TimeoutError:
            print(
                f"[{symbol}] No reply to buy request - outcome UNKNOWN. "
                "Do not assume the purchase failed."
            )
            return "unknown", None

        if "error" in buy_response:
            err = buy_response["error"]
            err_msg = err.get("message", "")

            print(
                f"[{symbol}] Buy error: {err_msg} "
                f"| code={err.get('code')} "
                f"| details={err.get('details')} "
                f"| proposal_id={proposal_id} | ask={ask_price}"
            )

            if (
                "unknown contract proposal" in err_msg.lower()
                or err.get("code") == "InvalidContractProposal"
            ):
                return "retry", None

            return "fail", None

        buy_data = buy_response.get("buy", {})
        contract_id = buy_data.get("contract_id")

        if not contract_id:
            print(
                f"[{symbol}] Buy response had no contract_id; "
                "outcome UNKNOWN."
            )
            return "unknown", None

        print(
            f"[{symbol}] Trade placed: contract_id={contract_id}, "
            f"stake=${stake:.2f}"
        )

        return "ok", contract_idasync def place_trade(
    account_id: str, symbol: str, contract_type: str
):
    """Calculate stake and place a trade safely."""

    global _trade_lock

    if _trade_lock is None:
        _trade_lock = asyncio.Lock()

    balance, currency = await get_account_balance(account_id)
    stake = calculate_stake(balance, currency)

    t_wait = time.time()

    async with _trade_lock:
        waited = time.time() - t_wait

        if waited > 1:
            print(
                f"[{symbol}] Waited {waited:.1f}s for the trade lock."
            )

        for attempt in range(BUY_RETRIES + 1):
            result, contract_id = await _attempt_trade(
                account_id, symbol, contract_type, stake
            )

            if result == "ok":
                return "ok", contract_id

            if result == "unknown":
                return "unknown", None

            if result == "fail":
                return "fail", None

            if attempt < BUY_RETRIES:
                print(
                    f"[{symbol}] Retrying with a fresh proposal "
                    f"({attempt + 1}/{BUY_RETRIES})..."
                )
                await asyncio.sleep(BUY_RETRY_DELAY)

        return "fail", None


# ====== PER-SYMBOL LOOP ======

async def sleep_until_next_candle():
    now = time.time()
    next_boundary = (
        (int(now // GRANULARITY) + 1) * GRANULARITY
    )

    await asyncio.sleep(
        next_boundary - now + CANDLE_CLOSE_DELAY
    )


async def run_symbol_loop(
    symbol: str, account_id_holder: dict
):
    last_traded_epoch = None
    last_event_key = None

    while True:
        try:
            if circuit_breaker_active():
                pass

            elif is_rest_period():
                print(
                    f"[{symbol}] Rest period active, "
                    "skipping this candle."
                )

            else:
                candles = await get_closed_candles(symbol)
                df = build_dataframe(candles)

                if len(df) < MIN_CANDLES:
                    print(
                        f"[{symbol}] Not enough closed candles "
                        f"({len(df)} / {MIN_CANDLES} needed)."
                    )

                else:
                    (
                        signal,
                        signal_epoch,
                        trigger,
                        event_key,
                    ) = check_signal(df)

                    if not signal:
                        print(
                            f"[{symbol}] No signal on this "
                            "closed candle."
                        )

                    elif event_key == last_event_key:
                        print(
                            f"[{symbol}] {signal} ({trigger}) is from "
                            "a crossover already traded, skipping."
                        )

                    elif (
                        last_traded_epoch is not None
                        and signal_epoch - last_traded_epoch
                        < COOLDOWN_CANDLES * GRANULARITY
                    ):
                        print(
                            f"[{symbol}] {signal} ({trigger}) skipped: "
                            "cooldown after last trade."
                        )

                    else:
                        print(
                            f"[{symbol}] Signal detected: {signal} "
                            f"via {trigger} - placing trade..."
                        )

                        try:
                            status, contract_id = await place_trade(
                                account_id_holder["id"],
                                symbol,
                                signal,
                            )

                            if status in ("ok", "unknown"):
                                last_traded_epoch = signal_epoch
                                last_event_key = event_key

                            if (
                                status == "ok"
                                and contract_id is not None
                            ):
                                try:
                                    profit = await monitor_contract(
                                        account_id_holder["id"],
                                        symbol,
                                        contract_id,
                                    )

                                    if profit is None:
                                        print(
                                            f"[{symbol}] Contract result "
                                            "unresolved; not counted "
                                            "as a win or loss."
                                        )

                                    else:
                                        record_confirmed_result(
                                            profit,
                                            symbol,
                                            contract_id,
                                        )

                                except Exception as monitor_err:
                                    print(
                                        f"[{symbol}] Could not confirm "
                                        f"contract result: {monitor_err}. "
                                        "Loss streak unchanged."
                                    )

                            elif status == "unknown":
                                print(
                                    f"[{symbol}] Purchase outcome "
                                    "unresolved; event consumed to "
                                    "avoid a duplicate purchase."
                                )

                        except Exception as trade_err:
                            print(
                                f"[{symbol}] Trade error: {trade_err}"
                            )

                            try:
                                account_id_holder["id"] = (
                                    await get_demo_account_id()
                                )

                            except Exception as refresh_err:
                                print(
                                    f"[{symbol}] Account re-discovery "
                                    f"failed: {refresh_err}"
                                )

        except Exception as e:
            print(f"[{symbol}] Error in strategy loop: {e}")

        await sleep_until_next_candle()


# ====== MAIN ======

async def main():
    missing = [
        name
        for name in ("DERIV_API_TOKEN", "DERIV_APP_ID")
        if not os.environ.get(name)
    ]

    if missing:
        print(
            f"ERROR: Missing environment variable(s): "
            f"{', '.join(missing)}"
        )
        print(
            "Set them in Railway under your service's Variables tab."
        )
        sys.exit(1)

    _load_risk_state()

    print("Discovering demo Options account...")

    account_id_holder = {
        "id": await get_demo_account_id()
    }

    print(
        f"Dynamic stake: {STAKE_PERCENT_BELOW:.0%} below "
        f"${BALANCE_TIER_USD:.2f}; "
        f"{STAKE_PERCENT_AT_ABOVE:.0%} at/above threshold, per trade."
    )

    print(
        f"Loss circuit breaker: {CONSECUTIVE_LOSS_LIMIT} consecutive "
        f"confirmed losses -> {LOSS_PAUSE_HOURS} hour pause."
    )

    if _risk_state.get("paused_until", 0) > time.time():
        print(
            f"Persisted pause active until epoch "
            f"{_risk_state['paused_until']:.0f}."
        )

    print(
        f"Bot started (15m breakout mode). "
        f"Running on: {', '.join(SYMBOLS)}"
    )

    print(
        f"Triggers: T1={TRIGGER1_ENABLED} "
        f"T2={TRIGGER2_ENABLED} | "
        f"Buffer={BREAKOUT_BUFFER_ENABLED} "
        f"Body={BODY_FILTER_ENABLED}"
    )

    if REST_ENABLED:
        print(
            f"Rest period: day {REST_START_DAY} "
            f"{REST_START_HOUR}:00 to day {REST_END_DAY} "
            f"{REST_END_HOUR}:00 ({REST_TIMEZONE})"
        )

    await asyncio.gather(*[
        run_symbol_loop(symbol, account_id_holder)
        for symbol in SYMBOLS
    ])


if __name__ == "__main__":
    asyncio.run(main())
