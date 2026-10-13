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

Definitions:
- Candle numbering: the signal candle is age 0, the one before it is age 1.
  A cross "on candle k" means the oscillator went from the wrong side of
  zero to the right side on that candle. The first cross counts as candle 0
  of the CROSS_GAP window, so CROSS_GAP = 2 means the two crosses can be at
  most 2 candles apart.
- The latest cross must be at most MOMENTUM_MAX_AGE candles old at the signal
  candle, and the crossed indicator must still be on the correct side of zero.
- Saucer (bullish): the last SAUCER_BARS bars of the indicator are each higher
  than the one before, preceded by a falling bar, and the total rise from the
  low bar is at least SAUCER_MIN_ATR x ATR. Bearish is the mirror image.
  Based on bar HEIGHT changes, not platform colors.
- Duplicate protection: one trade per crossover event, plus a per-symbol
  cooldown of COOLDOWN_CANDLES candles after a successful trade.
- Optional filters: breakout buffer (ATR-based) and candle body strength.
- Rest period: no trades Friday 6 PM to Saturday 6 PM (Jamaica time).

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
API_TOKEN     = os.environ.get("DERIV_API_TOKEN")
APP_ID        = os.environ.get("DERIV_APP_ID")

SYMBOLS       = ["R_25", "R_75", "R_100"]
GRANULARITY   = 900          # 15-minute candles
CANDLE_COUNT  = 110
STAKE         = 10
DURATION      = 5
DURATION_UNIT = "m"

# --- AO / AC settings ---
AC_SMA_PERIOD = 5            # AC = AO - SMA(AO, 5)  (standard Bill Williams)
MIN_CANDLES   = 50           # AO needs 34, AC needs ~38; extra buffer
ATR_PERIOD    = 14

# --- Momentum triggers ---
TRIGGER1_ENABLED   = True    # both AO and AC cross zero close together
TRIGGER2_ENABLED   = True    # one crosses zero + other shows a loose saucer
CROSS_GAP          = 2       # max candles between the AO and AC crosses
MOMENTUM_MAX_AGE   = 2       # latest cross must be at most this old
SAUCER_BARS        = 2       # consecutive rising/falling bars after a reversal bar
SAUCER_MIN_ATR     = 0.05    # min total move of the saucer, as a fraction of ATR

# --- Structure filter ---
STRUCTURE_LOOKBACK = 25      # candles to look back for swing high/low

# --- Optional false-breakout filters (turn on one at a time and compare) ---
BREAKOUT_BUFFER_ENABLED = False
BREAKOUT_BUFFER_ATR     = 0.25   # close must be beyond the level by this x ATR
BODY_FILTER_ENABLED     = False
MIN_BODY_RATIO          = 0.6    # candle body must be >= 60% of its full range

# --- Duplicate protection ---
COOLDOWN_CANDLES = 3         # per-symbol pause after a successful trade

# --- Rest period filter ---
# No trades between REST_START and REST_END (local time in REST_TIMEZONE).
# Days: Monday=0 ... Friday=4, Saturday=5, Sunday=6. Hours are 24-hour clock.
REST_ENABLED    = True
REST_TIMEZONE   = "America/Jamaica"   # UTC-5, no daylight saving
REST_START_DAY  = 4                   # Friday
REST_START_HOUR = 18                  # 6 PM
REST_END_DAY    = 5                   # Saturday
REST_END_HOUR   = 18                  # 6 PM

# --- Timing / robustness ---
CANDLE_CLOSE_DELAY = 2       # seconds after the boundary before checking
WS_TIMEOUT         = 15      # seconds to wait for any WebSocket reply
BUY_RETRIES        = 2       # extra attempts after "Unknown contract proposal"
BUY_RETRY_DELAY    = 1       # seconds between those attempts

ACCOUNTS_ENDPOINT = "https://api.derivws.com/trading/v1/options/accounts"
PUBLIC_WS_URL     = "wss://api.derivws.com/trading/v1/options/ws/public"


def get_headers():
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": str(APP_ID),
        "Content-Type": "application/json",
    }


# ====== REST PERIOD FILTER ======

def _local_now() -> datetime:
    """Current time in REST_TIMEZONE. Falls back to fixed UTC-5 if the
    timezone database isn't available on the server."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(REST_TIMEZONE))
        except Exception:
            pass
    return datetime.now(timezone(timedelta(hours=-5)))


def is_rest_period() -> bool:
    """True if the current local time falls inside the rest window."""
    if not REST_ENABLED:
        return False

    now = _local_now()
    now_min   = now.weekday() * 1440 + now.hour * 60 + now.minute
    start_min = REST_START_DAY * 1440 + REST_START_HOUR * 60
    end_min   = REST_END_DAY * 1440 + REST_END_HOUR * 60

    if start_min <= end_min:
        return start_min <= now_min < end_min
    return now_min >= start_min or now_min < end_min


# ====== ACCOUNT DISCOVERY ======

async def get_demo_account_id() -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(ACCOUNTS_ENDPOINT, headers=get_headers()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Accounts request failed ({resp.status}): {body}")
            data = await resp.json()
            accounts = data.get("data", [])

            if not accounts:
                raise RuntimeError(
                    "No Options accounts found. Make sure your PAT has the "
                    "'trade' scope and belongs to the same Deriv login as "
                    "your demo Options account."
                )

            for acc in accounts:
                if acc.get("account_type") == "demo" and acc.get("status") == "active":
                    account_id = acc["account_id"]
                    print(f"Found demo Options account: {account_id} "
                          f"(balance: {acc['balance']} {acc['currency']})")
                    return account_id

            print("Available accounts:")
            for acc in accounts:
                print(f"  {acc.get('account_id')} | type: {acc.get('account_type')} "
                      f"| status: {acc.get('status')}")
            raise RuntimeError("No active demo Options account found.")


async def get_fresh_otp_url(account_id: str) -> str:
    otp_endpoint = (
        f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(otp_endpoint, headers=get_headers()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"OTP request failed ({resp.status}): {body}")
            data = await resp.json()
            print("Fresh OTP URL obtained.")
            return data["data"]["url"]


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
                await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT)
            )
            if "error" in response:
                raise RuntimeError(
                    response["error"].get("message", "Unknown Deriv error")
                )
            if response.get("msg_type") == "candles":
                candles = response["candles"]
                closed = candles[:-1]   # drop the still-forming candle
                print(f"[{symbol}] Fetched {len(candles)} candles, "
                      f"using {len(closed)} closed.")
                return closed


def build_dataframe(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)
    return df


def candle_id(df: pd.DataFrame, idx: int) -> int:
    """Epoch of the candle at negative index idx (falls back to position)."""
    if "epoch" in df.columns:
        return int(df["epoch"].iloc[idx])
    return len(df) + idx


# ====== MOMENTUM HELPERS ======

def last_cross(series: pd.Series, direction: str, max_back: int):
    """
    Age (in candles, 0 = signal candle) of the most recent zero-line cross in
    `direction` ("up" or "down") within the last max_back candles. The series
    must still be on the correct side of zero on the signal candle.
    Returns None if there is no such cross.
    """
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


def has_saucer(series: pd.Series, direction: str, atr: float) -> bool:
    """
    Loose saucer based on bar height changes:
      up:   a falling bar, then SAUCER_BARS consecutive rising bars, with the
            total rise from the low bar >= SAUCER_MIN_ATR x ATR.
      down: a rising bar, then SAUCER_BARS consecutive falling bars, with the
            total fall from the high bar >= SAUCER_MIN_ATR x ATR.
    The indicator may be above or below zero.
    """
    n = SAUCER_BARS
    diffs = series.diff()
    recent = diffs.iloc[-n:]
    before = diffs.iloc[-(n + 1)]
    min_move = SAUCER_MIN_ATR * atr

    if direction == "up":
        if not ((recent > 0).all() and before < 0):
            return False
        return (series.iloc[-1] - series.iloc[-(n + 1)]) >= min_move
    else:
        if not ((recent < 0).all() and before > 0):
            return False
        return (series.iloc[-(n + 1)] - series.iloc[-1]) >= min_move


def detect_momentum(df, ao, ac, direction: str, atr: float):
    """
    Returns (trigger_name, cross_candle_epoch) if a momentum setup exists in
    `direction`, otherwise None. The cross_candle_epoch identifies the
    crossover event so the same event is never traded twice.
    """
    lookback = MOMENTUM_MAX_AGE + CROSS_GAP
    k_ao = last_cross(ao, direction, lookback)
    k_ac = last_cross(ac, direction, lookback)

    # Trigger 1: both cross zero within CROSS_GAP candles of each other
    if (TRIGGER1_ENABLED and k_ao is not None and k_ac is not None
            and abs(k_ao - k_ac) <= CROSS_GAP
            and min(k_ao, k_ac) <= MOMENTUM_MAX_AGE):
        newest = min(k_ao, k_ac)
        return "T1_both_cross", candle_id(df, -1 - newest)

    # Trigger 2: one crosses zero, the other shows a loose saucer
    if TRIGGER2_ENABLED:
        for crossed_k, other in ((k_ao, ac), (k_ac, ao)):
            if (crossed_k is not None and crossed_k <= MOMENTUM_MAX_AGE
                    and has_saucer(other, direction, atr)):
                return "T2_cross_plus_saucer", candle_id(df, -1 - crossed_k)

    return None


# ====== STRUCTURE + FILTERS ======

def broke_level(df: pd.DataFrame, direction: str, atr: float) -> bool:
    """
    Signal candle closes beyond the swing high (up) or swing low (down) of the
    prior STRUCTURE_LOOKBACK candles, plus an optional ATR buffer.
    """
    window = df.iloc[-(STRUCTURE_LOOKBACK + 1):-1]
    if len(window) < STRUCTURE_LOOKBACK:
        return False

    buffer = BREAKOUT_BUFFER_ATR * atr if BREAKOUT_BUFFER_ENABLED else 0.0
    close = df["close"].iloc[-1]

    if direction == "up":
        return close > window["high"].max() + buffer
    return close < window["low"].min() - buffer


def body_is_strong(df: pd.DataFrame, direction: str) -> bool:
    """Breakout candle body is a decent share of its range and in direction."""
    if not BODY_FILTER_ENABLED:
        return True
    c = df.iloc[-1]
    candle_range = c["high"] - c["low"]
    if candle_range <= 0:
        return False
    if (abs(c["close"] - c["open"]) / candle_range) < MIN_BODY_RATIO:
        return False
    return c["close"] > c["open"] if direction == "up" else c["close"] < c["open"]


# ====== SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    """
    Returns (signal, signal_epoch, trigger_name, event_key).
    signal is "CALL" / "PUT" / None.
    event_key identifies the crossover event, used for duplicate protection.
    """
    signal_epoch = candle_id(df, -1)

    ao = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    ).awesome_oscillator()
    ac = ao - ao.rolling(AC_SMA_PERIOD).mean()
    atr = ta.volatility.AverageTrueRange(
        high=df["high"], low=df["low"], close=df["close"], window=ATR_PERIOD
    ).average_true_range().iloc[-1]

    close_price = df["close"].iloc[-1]

    for direction, signal in (("up", "CALL"), ("down", "PUT")):
        momentum = detect_momentum(df, ao, ac, direction, atr)
        if momentum is None:
            continue

        trigger, cross_epoch = momentum
        level = "resistance" if direction == "up" else "support"

        if not broke_level(df, direction, atr):
            print(f"  {trigger} {direction} momentum seen but price "
                  f"({close_price:.4f}) hasn't broken {level}, skipping.")
            continue
        if not body_is_strong(df, direction):
            print(f"  {trigger} {direction} breakout rejected: weak candle body.")
            continue

        return signal, signal_epoch, trigger, (direction, cross_epoch)

    return None, signal_epoch, None, None


# ====== PLACE TRADE ======

async def _attempt_trade(account_id: str, symbol: str, contract_type: str) -> str:
    """
    One full attempt: fresh OTP, proposal, buy.
    Returns "ok", "retry" (Deriv said the proposal was unknown) or "fail".
    """
    otp_url = await get_fresh_otp_url(account_id)

    proposal_request = {
        "proposal": 1,
        "amount": STAKE,
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
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT))
            if msg.get("msg_type") == "proposal":
                if "error" in msg:
                    err = msg["error"]
                    print(f"[{symbol}] Proposal error: {err.get('message')} "
                          f"| code={err.get('code')} | details={err.get('details')}")
                    return "fail"
                proposal_id = msg["proposal"]["id"]
                ask_price   = msg["proposal"]["ask_price"]
                print(f"[{symbol}] Proposal received - ID: {proposal_id}, "
                      f"Ask: {ask_price}")
                break

        await ws.send(json.dumps({"buy": proposal_id, "price": ask_price}))
        buy_response = json.loads(
            await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT)
        )
        if "error" in buy_response:
            err = buy_response["error"]
            err_msg = err.get("message", "")
            print(f"[{symbol}] Buy error: {err_msg} | code={err.get('code')} "
                  f"| details={err.get('details')} | proposal_id={proposal_id} "
                  f"| ask={ask_price}")
            if "unknown contract proposal" in err_msg.lower():
                return "retry"
            return "fail"
        print(f"[{symbol}] Trade placed successfully:", buy_response)
        return "ok"


async def place_trade(account_id: str, symbol: str, contract_type: str) -> bool:
    """
    Returns True only if the buy succeeded. If Deriv rejects the buy with
    "Unknown contract proposal", retry with a brand-new proposal (up to
    BUY_RETRIES extra attempts). Only that specific error is retried, so a
    trade is never sent twice after a real buy.
    """
    for attempt in range(BUY_RETRIES + 1):
        result = await _attempt_trade(account_id, symbol, contract_type)
        if result == "ok":
            return True
        if result == "fail":
            return False
        if attempt < BUY_RETRIES:
            print(f"[{symbol}] Retrying with a fresh proposal "
                  f"({attempt + 1}/{BUY_RETRIES})...")
            await asyncio.sleep(BUY_RETRY_DELAY)
    return False


# ====== PER-SYMBOL LOOP ======

async def sleep_until_next_candle():
    now = time.time()
    next_boundary = (int(now // GRANULARITY) + 1) * GRANULARITY
    await asyncio.sleep(next_boundary - now + CANDLE_CLOSE_DELAY)


async def run_symbol_loop(symbol: str, account_id_holder: dict):
    last_traded_epoch = None
    last_event_key = None

    while True:
        try:
            if is_rest_period():
                print(f"[{symbol}] Rest period active, skipping this candle.")
            else:
                candles = await get_closed_candles(symbol)
                df = build_dataframe(candles)

                if len(df) < MIN_CANDLES:
                    print(f"[{symbol}] Not enough closed candles "
                          f"({len(df)} / {MIN_CANDLES} needed).")
                else:
                    signal, signal_epoch, trigger, event_key = check_signal(df)

                    if not signal:
                        print(f"[{symbol}] No signal on this closed candle.")
                    elif event_key == last_event_key:
                        print(f"[{symbol}] {signal} ({trigger}) is from a "
                              f"crossover already traded, skipping.")
                    elif (last_traded_epoch is not None and
                          signal_epoch - last_traded_epoch
                          < COOLDOWN_CANDLES * GRANULARITY):
                        print(f"[{symbol}] {signal} ({trigger}) skipped: "
                              f"cooldown after last trade.")
                    else:
                        print(f"[{symbol}] Signal detected: {signal} via "
                              f"{trigger} - placing trade...")
                        try:
                            ok = await place_trade(
                                account_id_holder["id"], symbol, signal
                            )
                            if ok:
                                last_traded_epoch = signal_epoch
                                last_event_key = event_key
                        except Exception as trade_err:
                            print(f"[{symbol}] Trade error: {trade_err}")
                            try:
                                account_id_holder["id"] = await get_demo_account_id()
                            except Exception as refresh_err:
                                print(f"[{symbol}] Account re-discovery failed: "
                                      f"{refresh_err}")

        except Exception as e:
            print(f"[{symbol}] Error in strategy loop: {e}")

        await sleep_until_next_candle()


# ====== MAIN ======

async def main():
    missing = [v for v in ("DERIV_API_TOKEN", "DERIV_APP_ID") if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing environment variable(s): {', '.join(missing)}")
        print("Set them in Railway under your service's Variables tab.")
        sys.exit(1)

    print("Discovering demo Options account...")
    account_id_holder = {"id": await get_demo_account_id()}

    print(f"Bot started (15m breakout mode). Running on: {', '.join(SYMBOLS)}")
    print(f"Triggers: T1={TRIGGER1_ENABLED} T2={TRIGGER2_ENABLED} | "
          f"Buffer={BREAKOUT_BUFFER_ENABLED} Body={BODY_FILTER_ENABLED}")
    if REST_ENABLED:
        print(f"Rest period: day {REST_START_DAY} {REST_START_HOUR}:00 to "
              f"day {REST_END_DAY} {REST_END_HOUR}:00 ({REST_TIMEZONE})")

    await asyncio.gather(*[
        run_symbol_loop(symbol, account_id_holder) for symbol in SYMBOLS
    ])


if __name__ == "__main__":
    asyncio.run(main())
