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
- Choppy-market filter: skips breakouts when ADX is low or ATR is well below
  its recent average (sideways / quiet markets).
- Loss circuit breaker: pauses all trading after N losses in a row (for a few
  hours) or when the day's net loss reaches a cap (until the next local day).
  Results are read back from Deriv after each contract expires.
- Trade log: every settled trade is appended to a CSV. Run
  `python bot-4.py report` for win rate by symbol, trigger, ADX band and hour.
- Stake ladder (per symbol): each symbol steps through fixed percentages of
  BANKROLL - down after a loss, up after a win - between a floor and a cap.
  The stake is never doubled to win losses back. A total-at-risk limit caps
  the combined stake of all open trades across the three symbols.

Authentication (Deriv API v1):
- GET accounts -> find demo account ID
- POST OTP only when a signal fires, then connect and trade immediately

Environment variables (Railway -> Variables):
  DERIV_API_TOKEN, DERIV_APP_ID
"""

import asyncio
import csv
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
BANKROLL      = 10           # the amount the percentages below apply to
DURATION      = 5
DURATION_UNIT = "m"

# --- AO / AC settings ---
AC_SMA_PERIOD = 5            # AC = AO - SMA(AO, 5)  (standard Bill Williams)
MIN_CANDLES   = 50           # AO needs 34, AC needs ~38; extra buffer
ATR_PERIOD    = 14

# --- Momentum triggers ---
TRIGGER1_ENABLED   = True    # both AO and AC cross zero close together
TRIGGER2_ENABLED   = False    # one crosses zero + other shows a loose saucer
CROSS_GAP          = 3       # max candles between the AO and AC crosses
MOMENTUM_MAX_AGE   = 3       # latest cross must be at most this old
SAUCER_BARS        = 2       # consecutive rising/falling bars after a reversal bar
SAUCER_MIN_ATR     = 0.05    # min total move of the saucer, as a fraction of ATR

# --- Structure filter ---
STRUCTURE_LOOKBACK = 25      # candles to look back for swing high/low
BREAKOUT_WINDOW    = 1       # breakout may be on the signal candle or up to this
                             # many candles before it (price must still be
                             # beyond the broken level on the signal candle)

# --- Optional false-breakout filters (turn on one at a time and compare) ---
BREAKOUT_BUFFER_ENABLED = False
BREAKOUT_BUFFER_ATR     = 0.25   # close must be beyond the level by this x ATR
BODY_FILTER_ENABLED     = False
MIN_BODY_RATIO          = 0.6    # candle body must be >= 60% of its full range

# --- Duplicate protection ---
COOLDOWN_CANDLES = 4         # per-symbol pause after a trade (> MOMENTUM_MAX_AGE)

# --- Choppy-market filter (skip breakouts in sideways / quiet markets) ---
ADX_FILTER_ENABLED = True
ADX_PERIOD         = 14
ADX_MIN            = 20      # skip if ADX is below this (low ADX = ranging market)
ATR_RATIO_ENABLED  = True
ATR_AVG_PERIOD     = 30      # compare ATR to its own average over this many candles
ATR_RATIO_MIN      = 0.9     # skip if ATR is below this x its recent average

# --- Stake ladder (each symbol has its own position on the ladder) ---
# Percent of BANKROLL. A win moves one step right, a loss one step left.
# First rung 20% and last rung 80% are the floor and the hard cap.
STAKE_LADDER_PCT   = [20, 30, 40, 60, 80]
STAKE_START_PCT    = 40          # every symbol starts here, and resets here
RESET_LADDER_AFTER_PAUSE = True  # back to the start rung after a breaker pause
MAX_TOTAL_RISK_PCT = 60          # combined stake of all open trades, % of BANKROLL
MIN_STAKE          = 1.0         # Deriv's minimum stake - never go below this

# --- Loss circuit breaker ---
BREAKER_ENABLED         = True
MAX_CONSEC_LOSSES       = 3      # pause after this many losses in a row (0 = off)
CONSEC_LOSS_PAUSE_HOURS = 3      # length of that pause
MAX_DAILY_LOSS          = 8      # pause until the next local day when the day's net
                                 # loss reaches this amount (0 = off). Open trades
                                 # count as possible losses, so the cap holds.

# --- Per-symbol control, trade log, saved state ---
DISABLED_SYMBOLS = []            # e.g. ["R_100"] to switch a symbol off
TRADE_LOG_FILE   = os.environ.get("TRADE_LOG_FILE", "trade_log.csv")
STATE_FILE       = os.environ.get("BOT_STATE_FILE", "bot_state.json")
SETTLE_BUFFER    = 5             # seconds after expiry before checking the result
SETTLE_POLLS     = 8             # how many times to try reading the result
SETTLE_POLL_GAP  = 5             # seconds between those tries
REPORT_MIN_TRADES      = 50      # report flags symbol/trigger groups below this
REPORT_MIN_HOUR_TRADES = 100     # hour-of-day groups need far more before meaning anything

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

_trade_lock = None           # created on first use, inside the running event loop
_background_tasks = set()    # keeps result-tracking tasks alive
DURATION_SECONDS = DURATION * {"t": 2, "s": 1, "m": 60, "h": 3600, "d": 86400}.get(
    DURATION_UNIT, 60
)

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
                last = candles[-1]
                # Candle epoch is its open time, so it is still forming if
                # open time + granularity is in the future. If the new candle
                # has not started yet, the last one is already closed - keep it.
                forming = ("epoch" not in last or
                           last["epoch"] + GRANULARITY > time.time())
                closed = candles[:-1] if forming else candles
                print(f"[{symbol}] Fetched {len(candles)} candles, using "
                      f"{len(closed)} closed"
                      f"{'' if forming else ' (latest already closed)'}.")
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

def broke_level(df: pd.DataFrame, direction: str, atr: float):
    """
    Looks for a close beyond the swing high (up) or swing low (down) of the
    STRUCTURE_LOOKBACK candles before it, plus an optional ATR buffer. The
    breakout candle may be the signal candle (offset 0) or up to
    BREAKOUT_WINDOW candles earlier, but the signal candle's close must still
    be beyond the broken level (price hasn't fallen back inside).
    Returns the offset of the breakout candle, or None.
    """
    buffer = BREAKOUT_BUFFER_ATR * atr if BREAKOUT_BUFFER_ENABLED else 0.0
    close_now = df["close"].iloc[-1]

    for j in range(BREAKOUT_WINDOW + 1):
        window = df.iloc[-(STRUCTURE_LOOKBACK + 1 + j):-(1 + j)]
        if len(window) < STRUCTURE_LOOKBACK:
            continue
        close_j = df["close"].iloc[-1 - j]

        if direction == "up":
            level = window["high"].max()
            if close_j > level + buffer and close_now > level + buffer:
                return j
        else:
            level = window["low"].min()
            if close_j < level - buffer and close_now < level - buffer:
                return j
    return None


def body_is_strong(df: pd.DataFrame, direction: str, offset: int = 0) -> bool:
    """Breakout candle body is a decent share of its range and in direction."""
    if not BODY_FILTER_ENABLED:
        return True
    c = df.iloc[-1 - offset]
    candle_range = c["high"] - c["low"]
    if candle_range <= 0:
        return False
    if (abs(c["close"] - c["open"]) / candle_range) < MIN_BODY_RATIO:
        return False
    return c["close"] > c["open"] if direction == "up" else c["close"] < c["open"]


# ====== CHOPPY-MARKET FILTER ======

def market_conditions(df: pd.DataFrame, atr_series: pd.Series) -> dict:
    """ADX and ATR-vs-average at the signal candle (None if unavailable)."""
    adx = None
    try:
        adx_val = ta.trend.ADXIndicator(
            high=df["high"], low=df["low"], close=df["close"], window=ADX_PERIOD
        ).adx().iloc[-1]
        if not pd.isna(adx_val):
            adx = float(adx_val)
    except Exception as e:
        print(f"  ADX calculation failed: {e}")

    ratio = None
    avg = atr_series.rolling(ATR_AVG_PERIOD).mean().iloc[-1]
    if not pd.isna(avg) and avg > 0:
        ratio = float(atr_series.iloc[-1] / avg)
    return {"adx": adx, "atr_ratio": ratio}


def market_is_trending(info: dict):
    """Returns (ok, reason). A missing value never blocks a trade."""
    adx, ratio = info.get("adx"), info.get("atr_ratio")
    if ADX_FILTER_ENABLED and adx is not None and adx < ADX_MIN:
        return False, f"choppy market (ADX {adx:.1f} < {ADX_MIN})"
    if ATR_RATIO_ENABLED and ratio is not None and ratio < ATR_RATIO_MIN:
        return False, (f"quiet market (ATR is {ratio:.2f}x its "
                       f"{ATR_AVG_PERIOD}-candle average, min {ATR_RATIO_MIN})")
    return True, ""


# ====== SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    """
    Returns (signal, signal_epoch, trigger_name, event_key, market_info).
    signal is "CALL" / "PUT" / None.
    event_key identifies the crossover event, used for duplicate protection.
    """
    signal_epoch = candle_id(df, -1)

    ao = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    ).awesome_oscillator()
    ac = ao - ao.rolling(AC_SMA_PERIOD).mean()
    atr_series = ta.volatility.AverageTrueRange(
        high=df["high"], low=df["low"], close=df["close"], window=ATR_PERIOD
    ).average_true_range()
    atr = atr_series.iloc[-1]
    info = market_conditions(df, atr_series)

    close_price = df["close"].iloc[-1]

    for direction, signal in (("up", "CALL"), ("down", "PUT")):
        momentum = detect_momentum(df, ao, ac, direction, atr)
        if momentum is None:
            continue

        trigger, cross_epoch = momentum
        level = "resistance" if direction == "up" else "support"

        offset = broke_level(df, direction, atr)
        if offset is None:
            print(f"  {trigger} {direction} momentum seen but price "
                  f"({close_price:.4f}) hasn't broken {level}, skipping.")
            continue
        if offset > 0:
            print(f"  {trigger} {direction}: breakout was {offset} candle(s) "
                  f"ago and price is still beyond {level}.")
        trending, why = market_is_trending(info)
        if not trending:
            print(f"  {trigger} {direction} breakout rejected: {why}.")
            continue
        if not body_is_strong(df, direction, offset):
            print(f"  {trigger} {direction} breakout rejected: weak candle body.")
            continue

        return signal, signal_epoch, trigger, (direction, cross_epoch), info

    return None, signal_epoch, None, None, info


# ====== PLACE TRADE ======

async def _attempt_trade(account_id: str, symbol: str, contract_type: str,
                         stake: float):
    """
    One full attempt: fresh OTP, proposal, buy.
    Returns (status, info). status is "ok", "unknown" (the buy may or may not
    have gone through), "retry" (Deriv said the proposal was unknown) or "fail".
    info carries ask_price / contract_id / buy_price when known.
    """
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
    info = {}

    async with websockets.connect(otp_url) as ws:
        await ws.send(json.dumps(proposal_request))
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT))
            if "error" in msg:
                err = msg["error"]
                print(f"[{symbol}] Proposal error: {err.get('message')} "
                      f"| code={err.get('code')} | details={err.get('details')}")
                return "fail", info
            if msg.get("msg_type") == "proposal":
                proposal = msg.get("proposal", {})
                proposal_id = proposal.get("id")
                ask_price   = proposal.get("ask_price")
                if not proposal_id or ask_price is None:
                    print(f"[{symbol}] Proposal response missing ID or price.")
                    return "fail", info
                print(f"[{symbol}] Proposal received - ID: {proposal_id}, "
                      f"Ask: {ask_price}")
                break

        info["ask_price"] = ask_price
        await ws.send(json.dumps({"buy": proposal_id, "price": ask_price}))
        try:
            while True:
                buy_response = json.loads(
                    await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT)
                )
                if buy_response.get("msg_type") == "buy" or "error" in buy_response:
                    break
        except Exception as e:
            # Timeout OR a dropped connection after the buy was sent: the trade
            # may exist, so never send this signal a second time.
            print(f"[{symbol}] No reply to the buy request ({type(e).__name__}) "
                  f"- outcome UNKNOWN. Check your Deriv account before "
                  f"assuming it failed.")
            return "unknown", info
        if "error" in buy_response:
            err = buy_response["error"]
            err_msg = err.get("message", "")
            print(f"[{symbol}] Buy error: {err_msg} | code={err.get('code')} "
                  f"| details={err.get('details')} | proposal_id={proposal_id} "
                  f"| ask={ask_price}")
            if ("unknown contract proposal" in err_msg.lower()
                    or err.get("code") == "InvalidContractProposal"):
                return "retry", info
            return "fail", info

        buy = buy_response.get("buy")
        if isinstance(buy, dict):
            info["contract_id"] = buy.get("contract_id")
            info["buy_price"] = buy.get("buy_price", ask_price)
        print(f"[{symbol}] Trade placed successfully:", buy_response)
        return "ok", info


async def place_trade(account_id: str, symbol: str, contract_type: str,
                      stake: float):
    """
    Returns (placed, info). placed is True if the buy succeeded or its outcome
    is unknown (so the signal is treated as used). If Deriv rejects the buy
    with "Unknown contract proposal", retry with a brand-new proposal (up to
    BUY_RETRIES extra attempts). Only that error is retried, so a trade is
    never sent twice after a real buy.
    """
    global _trade_lock
    if _trade_lock is None:
        _trade_lock = asyncio.Lock()

    # Only one symbol trades at a time, so simultaneous signals on a candle
    # boundary never overlap their proposal/buy requests.
    t_wait = time.time()
    async with _trade_lock:
        waited = time.time() - t_wait
        if waited > 1:
            print(f"[{symbol}] Waited {waited:.1f}s for the trade lock.")
        for attempt in range(BUY_RETRIES + 1):
            status, info = await _attempt_trade(account_id, symbol, contract_type, stake)
            if status in ("ok", "unknown"):
                return True, info
            if status == "fail":
                return False, info
            if attempt < BUY_RETRIES:
                print(f"[{symbol}] Retrying with a fresh proposal "
                      f"({attempt + 1}/{BUY_RETRIES})...")
                await asyncio.sleep(BUY_RETRY_DELAY)
        return False, {}


# ====== STAKE LADDER ======

def stake_for_pct(pct: float) -> float:
    return round(max(MIN_STAKE, BANKROLL * pct / 100.0), 2)


def start_rung() -> int:
    if STAKE_START_PCT in STAKE_LADDER_PCT:
        return STAKE_LADDER_PCT.index(STAKE_START_PCT)
    return min(range(len(STAKE_LADDER_PCT)),
               key=lambda i: abs(STAKE_LADDER_PCT[i] - STAKE_START_PCT))


# ====== RISK MANAGER (LOSS CIRCUIT BREAKER) ======

def _next_local_midnight_ts() -> float:
    now = _local_now()
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return nxt.timestamp()


class RiskManager:
    """
    Tracks results across all symbols and pauses trading when the loss rules
    are hit. State is saved to STATE_FILE so a restart does not reset a pause.
    (On Railway, point BOT_STATE_FILE / TRADE_LOG_FILE at a mounted volume,
    otherwise a redeploy wipes them.)
    """

    def __init__(self):
        self.day = _local_now().date().isoformat()
        self.daily_pnl = 0.0
        self.streak = 0
        self.pause_until = 0.0
        self.pause_reason = ""
        self.open_stakes = 0.0          # stake currently riding on unsettled trades
        self.rungs = {}                 # {symbol: position on the stake ladder}
        self.symbol_stats = {}          # since this start: {symbol: {w, l, pnl}}
        self._load()
        self._roll_day()

    def _load(self):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            self.day = s.get("day", self.day)
            self.daily_pnl = float(s.get("daily_pnl", 0.0))
            self.streak = int(s.get("streak", 0))
            self.pause_until = float(s.get("pause_until", 0.0))
            self.pause_reason = s.get("pause_reason", "")
            saved = s.get("rungs", {})
            top = len(STAKE_LADDER_PCT) - 1
            self.rungs = {k: max(0, min(int(v), top)) for k, v in saved.items()}
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"Could not read {STATE_FILE}: {e}")

    def _save(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump({
                    "day": self.day, "daily_pnl": self.daily_pnl,
                    "streak": self.streak, "pause_until": self.pause_until,
                    "pause_reason": self.pause_reason,
                    "rungs": self.rungs,
                }, f)
        except Exception as e:
            print(f"Could not save {STATE_FILE}: {e}")

    def _roll_day(self):
        today = _local_now().date().isoformat()
        if today != self.day:
            print(f"New local day ({today}): daily P&L and loss streak reset.")
            self.day = today
            self.daily_pnl = 0.0
            self.streak = 0
            self._save()

    def _pause(self, until_ts: float, reason: str):
        if until_ts <= self.pause_until:
            return
        self.pause_until = until_ts
        self.pause_reason = reason
        if RESET_LADDER_AFTER_PAUSE:
            self.rungs = {}
            print("Stake ladder reset to the starting rung for every symbol.")
        resume = _local_now() + timedelta(seconds=until_ts - time.time())
        print(f"*** CIRCUIT BREAKER: {reason}. Trading paused until "
              f"{resume.strftime('%a %H:%M')} ({REST_TIMEZONE}). ***")

    def current_stake(self, symbol: str) -> float:
        rung = self.rungs.get(symbol, start_rung())
        return stake_for_pct(STAKE_LADDER_PCT[rung])

    def risk_limit(self) -> float:
        return BANKROLL * MAX_TOTAL_RISK_PCT / 100.0

    def can_trade(self, new_trade: bool = False, symbol: str = None):
        """Returns (allowed, reason). new_trade=True also applies the hard cap."""
        if not BREAKER_ENABLED:
            return True, ""
        self._roll_day()
        now = time.time()
        if now < self.pause_until:
            mins = (self.pause_until - now) / 60
            return False, f"{self.pause_reason}; resumes in {mins:.0f} min"
        if new_trade and symbol is not None:
            stake = self.current_stake(symbol)
            if MAX_TOTAL_RISK_PCT > 0 and self.open_stakes + stake > self.risk_limit() + 1e-9:
                return False, (f"total at-risk limit: {self.open_stakes:.2f} already "
                               f"open + {stake:.2f} would pass the "
                               f"{self.risk_limit():.2f} limit "
                               f"({MAX_TOTAL_RISK_PCT}% of {BANKROLL})")
            if (MAX_DAILY_LOSS > 0
                    and self.daily_pnl - self.open_stakes - stake < -MAX_DAILY_LOSS):
                return False, (f"open trades could push today past the "
                               f"{MAX_DAILY_LOSS} loss cap "
                               f"(day P&L {self.daily_pnl:+.2f}, "
                               f"{self.open_stakes:.2f} at risk)")
        return True, ""

    def record_result(self, symbol: str, profit: float):
        self._roll_day()
        self.daily_pnl += profit
        if profit < 0:
            self.streak += 1
        elif profit > 0:
            self.streak = 0

        top = len(STAKE_LADDER_PCT) - 1
        rung = self.rungs.get(symbol, start_rung())
        if profit > 0:
            rung = min(top, rung + 1)
        elif profit < 0:
            rung = max(0, rung - 1)
        self.rungs[symbol] = rung

        st = self.symbol_stats.setdefault(symbol, {"w": 0, "l": 0, "pnl": 0.0})
        st["pnl"] += profit
        if profit > 0:
            st["w"] += 1
        elif profit < 0:
            st["l"] += 1

        if BREAKER_ENABLED:
            if MAX_DAILY_LOSS > 0 and self.daily_pnl <= -MAX_DAILY_LOSS:
                self._pause(_next_local_midnight_ts(),
                            f"daily loss cap reached ({self.daily_pnl:+.2f})")
            elif MAX_CONSEC_LOSSES > 0 and self.streak >= MAX_CONSEC_LOSSES:
                self._pause(time.time() + CONSEC_LOSS_PAUSE_HOURS * 3600,
                            f"{self.streak} losses in a row")
                self.streak = 0
        self._save()

    def status_line(self) -> str:
        paused = ""
        if time.time() < self.pause_until:
            paused = f" | PAUSED: {self.pause_reason}"
        nxt = ", ".join(f"{sym} {self.current_stake(sym):.2f}" for sym in SYMBOLS)
        return (f"day P&L {self.daily_pnl:+.2f} | loss streak {self.streak} | "
                f"next stakes: {nxt}{paused}")


# ====== RESULT TRACKING + TRADE LOG ======

TRADE_LOG_FIELDS = [
    "closed_local", "opened_local", "local_hour", "symbol", "direction",
    "trigger", "stake", "entry_ask", "contract_id", "adx", "atr_ratio",
    "result", "profit",
]


def log_trade(row: dict):
    try:
        new_file = not os.path.exists(TRADE_LOG_FILE)
        with open(TRADE_LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_LOG_FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"Could not write {TRADE_LOG_FILE}: {e}")


async def fetch_result(account_id: str, symbol: str, contract_id):
    """
    Waits for the contract to expire, then reads its profit from Deriv.
    Returns profit (float, negative for a loss) or None if it can't be read.
    """
    await asyncio.sleep(DURATION_SECONDS + SETTLE_BUFFER)
    for _ in range(SETTLE_POLLS):
        try:
            otp_url = await get_fresh_otp_url(account_id)
            async with websockets.connect(otp_url) as ws:
                await ws.send(json.dumps({
                    "proposal_open_contract": 1, "contract_id": contract_id,
                }))
                while True:
                    msg = json.loads(
                        await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT)
                    )
                    if "error" in msg:
                        print(f"[{symbol}] Result lookup error: "
                              f"{msg['error'].get('message')}")
                        break
                    poc = msg.get("proposal_open_contract")
                    if poc is None:
                        continue
                    if poc.get("is_sold") or poc.get("status") in ("won", "lost"):
                        profit = poc.get("profit")
                        if profit is None and (poc.get("sell_price") is not None
                                               and poc.get("buy_price") is not None):
                            profit = poc["sell_price"] - poc["buy_price"]
                        if profit is not None:
                            return float(profit)
                    break  # not settled yet; try again after a pause
        except Exception as e:
            print(f"[{symbol}] Result lookup failed: {type(e).__name__}: {e}")
        await asyncio.sleep(SETTLE_POLL_GAP)
    return None


async def settle_and_record(account_id: str, ctx: dict, trade_info: dict,
                            risk: RiskManager):
    """Background task: read the result, update the circuit breaker, log it."""
    symbol = ctx["symbol"]
    contract_id = trade_info.get("contract_id")
    profit = None
    try:
        if contract_id:
            profit = await fetch_result(account_id, symbol, contract_id)
        else:
            print(f"[{symbol}] No contract ID in the buy reply, so this "
                  f"trade's result can't be tracked.")
    except Exception as e:
        print(f"[{symbol}] Result tracking error: {e}")
    finally:
        risk.open_stakes = max(0.0, risk.open_stakes - ctx["stake"])

    if profit is None:
        result = "UNKNOWN"
    elif profit > 0:
        result = "WIN"
    elif profit < 0:
        result = "LOSS"
    else:
        result = "DRAW"

    if profit is not None:
        risk.record_result(symbol, profit)

    def fmt(v, nd):
        return "" if v is None else round(v, nd)

    log_trade({
        "closed_local": _local_now().strftime("%Y-%m-%d %H:%M:%S"),
        "opened_local": ctx["opened_local"],
        "local_hour": ctx["local_hour"],
        "symbol": symbol,
        "direction": ctx["direction"],
        "trigger": ctx["trigger"],
        "stake": ctx["stake"],
        "entry_ask": trade_info.get("ask_price", ""),
        "contract_id": contract_id or "",
        "adx": fmt(ctx.get("adx"), 1),
        "atr_ratio": fmt(ctx.get("atr_ratio"), 2),
        "result": result,
        "profit": "" if profit is None else round(profit, 2),
    })

    if profit is None:
        print(f"[{symbol}] Result UNKNOWN - not counted by the circuit breaker. "
              f"Check your Deriv account.")
    else:
        st = risk.symbol_stats.get(symbol, {"w": 0, "l": 0, "pnl": 0.0})
        print(f"[{symbol}] Result: {result} ({profit:+.2f}) | {symbol} since "
              f"start: {st['w']}W/{st['l']}L ({st['pnl']:+.2f}) | "
              f"{risk.status_line()}")


# ====== TRADE LOG REPORT  (python bot-4.py report) ======

def _summarize(df: pd.DataFrame, key, min_trades: int) -> pd.DataFrame:
    g = df.groupby(key, observed=True)
    out = pd.DataFrame({
        "trades": g.size(),
        "wins": g["is_win"].sum(),
        "profit": g["profit"].sum().round(2),
    })
    out["win_%"] = (100 * out["wins"] / out["trades"]).round(1)
    out["note"] = ["too few trades" if n < min_trades else ""
                   for n in out["trades"]]
    return out[["trades", "wins", "win_%", "profit", "note"]]


def print_report():
    if not os.path.exists(TRADE_LOG_FILE):
        print(f"No trade log found at {TRADE_LOG_FILE} yet.")
        return
    df = pd.read_csv(TRADE_LOG_FILE)
    df = df[df["result"].isin(["WIN", "LOSS"])].copy()
    if df.empty:
        print("No settled trades in the log yet.")
        return

    df["is_win"] = df["result"] == "WIN"
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0.0)
    df["adx"] = pd.to_numeric(df["adx"], errors="coerce")
    df["adx_band"] = pd.cut(df["adx"], bins=[0, 20, 25, 30, 100],
                            labels=["<20", "20-25", "25-30", "30+"], right=False)

    n = len(df)
    print(f"\nOVERALL: {n} trades | win rate {100 * df['is_win'].mean():.1f}% "
          f"| profit {df['profit'].sum():+.2f}")
    print("(Payouts are below 2x the stake, so the win rate must be above 50% "
          "just to break even.)")

    print("\nBY SYMBOL  (a symbol that keeps losing over many trades can go in "
          "DISABLED_SYMBOLS)")
    print(_summarize(df, "symbol", REPORT_MIN_TRADES).to_string())

    print("\nBY TRIGGER")
    print(_summarize(df, "trigger", REPORT_MIN_TRADES).to_string())

    if df["adx"].notna().any():
        print("\nBY ADX BAND  (use this to tune ADX_MIN)")
        print(_summarize(df.dropna(subset=["adx"]), "adx_band",
                         REPORT_MIN_TRADES).to_string())

    print("\nBY LOCAL HOUR  (these indices run 24/7 on an algorithm, so hourly "
          "differences are usually noise - don't act on them without hundreds "
          "of trades)")
    print(_summarize(df, "local_hour", REPORT_MIN_HOUR_TRADES).to_string())


# ====== PER-SYMBOL LOOP ======

async def sleep_until_next_candle():
    now = time.time()
    next_boundary = (int(now // GRANULARITY) + 1) * GRANULARITY
    await asyncio.sleep(next_boundary - now + CANDLE_CLOSE_DELAY)


async def run_symbol_loop(symbol: str, account_id_holder: dict, risk: RiskManager):
    last_traded_epoch = None
    last_event_key = None

    while True:
        try:
            if is_rest_period():
                print(f"[{symbol}] Rest period active, skipping this candle.")
            else:
                allowed, why = risk.can_trade()
                if not allowed:
                    print(f"[{symbol}] Circuit breaker: {why}. "
                          f"Skipping this candle.")
                else:
                    candles = await get_closed_candles(symbol)
                    df = build_dataframe(candles)

                    if len(df) < MIN_CANDLES:
                        print(f"[{symbol}] Not enough closed candles "
                              f"({len(df)} / {MIN_CANDLES} needed).")
                    else:
                        signal, signal_epoch, trigger, event_key, info = (
                            check_signal(df)
                        )

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
                            allowed, why = risk.can_trade(new_trade=True, symbol=symbol)
                            if not allowed:
                                print(f"[{symbol}] {signal} ({trigger}) skipped: "
                                      f"{why}.")
                            else:
                                stake = risk.current_stake(symbol)
                                print(f"[{symbol}] Signal detected: {signal} via "
                                      f"{trigger} - placing trade, stake {stake:.2f}...")
                                now_local = _local_now()
                                ctx = {
                                    "symbol": symbol,
                                    "stake": stake,
                                    "direction": signal,
                                    "trigger": trigger,
                                    "adx": info.get("adx"),
                                    "atr_ratio": info.get("atr_ratio"),
                                    "opened_local": now_local.strftime(
                                        "%Y-%m-%d %H:%M:%S"),
                                    "local_hour": now_local.hour,
                                }
                                try:
                                    ok, trade_info = await place_trade(
                                        account_id_holder["id"], symbol, signal,
                                        stake,
                                    )
                                    if ok:
                                        last_traded_epoch = signal_epoch
                                        last_event_key = event_key
                                        risk.open_stakes += stake
                                        task = asyncio.create_task(
                                            settle_and_record(
                                                account_id_holder["id"], ctx,
                                                trade_info, risk,
                                            )
                                        )
                                        _background_tasks.add(task)
                                        task.add_done_callback(
                                            _background_tasks.discard
                                        )
                                except Exception as trade_err:
                                    print(f"[{symbol}] Trade error: {trade_err}")
                                    try:
                                        account_id_holder["id"] = (
                                            await get_demo_account_id()
                                        )
                                    except Exception as refresh_err:
                                        print(f"[{symbol}] Account re-discovery "
                                              f"failed: {refresh_err}")

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

    active = [s for s in SYMBOLS if s not in DISABLED_SYMBOLS]
    if not active:
        print("ERROR: every symbol is in DISABLED_SYMBOLS - nothing to trade.")
        sys.exit(1)

    print("Discovering demo Options account...")
    account_id_holder = {"id": await get_demo_account_id()}
    risk = RiskManager()

    print(f"Bot started (15m breakout mode). Running on: {', '.join(active)}")
    if DISABLED_SYMBOLS:
        print(f"Disabled symbols: {', '.join(DISABLED_SYMBOLS)}")
    print(f"Triggers: T1={TRIGGER1_ENABLED} T2={TRIGGER2_ENABLED} | "
          f"Buffer={BREAKOUT_BUFFER_ENABLED} Body={BODY_FILTER_ENABLED}")
    print(f"Choppy filter: ADX>={ADX_MIN} ({ADX_FILTER_ENABLED}), "
          f"ATR ratio>={ATR_RATIO_MIN} ({ATR_RATIO_ENABLED}) | "
          f"Stake ladder (% of {BANKROLL}): {STAKE_LADDER_PCT}, start "
          f"{STAKE_START_PCT}%, per symbol | total at-risk limit "
          f"{MAX_TOTAL_RISK_PCT}%")
    if BREAKER_ENABLED:
        print(f"Circuit breaker: {MAX_CONSEC_LOSSES} losses in a row -> "
              f"{CONSEC_LOSS_PAUSE_HOURS}h pause | daily loss cap "
              f"{MAX_DAILY_LOSS} -> pause until next day")
        print(f"Risk state: {risk.status_line()}")
    if REST_ENABLED:
        print(f"Rest period: day {REST_START_DAY} {REST_START_HOUR}:00 to "
              f"day {REST_END_DAY} {REST_END_HOUR}:00 ({REST_TIMEZONE})")

    await asyncio.gather(*[
        run_symbol_loop(symbol, account_id_holder, risk) for symbol in active
    ])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        print_report()
    else:
        asyncio.run(main())
