"""
Deriv MACD + Awesome Oscillator Reversal Bot  (API v1)
-------------------------------------------------------
Strategy (run independently per symbol):
- MACD(12, 26, 9): the MACD line crossing the signal line while BELOW the
  zero line signals a possible bullish reversal; crossing while ABOVE zero
  signals a possible bearish reversal.
- Awesome Oscillator (AO): at least MIN_STREAK candles of the same color
  (preceding the flip candle), then a color flip on the signal candle confirms
  momentum has actually turned.
- A trade is only placed when both conditions align on the same CLOSED candle.

Multi-symbol:
- The bot now runs the same strategy concurrently and independently on
  Volatility 25, Volatility 75, and Volatility 100 Indices. Each symbol has
  its own candle fetch, signal check, and duplicate-candle protection so a
  signal on one symbol never affects another.

Authentication (new Deriv API v1):
- Step 1: GET /trading/v1/options/accounts to auto-discover demo account ID
- Step 2: POST to the OTP endpoint ONLY when a signal fires (OTP is one-time/
  short-lived, so we must request it fresh immediately before connecting)
- Step 3: Connect to that OTP WebSocket URL and place the trade immediately

Environment variables required (set in Railway → Variables tab):
  DERIV_API_TOKEN   Your Personal Access Token (PAT) from Deriv (needs trade scope)
  DERIV_APP_ID      Your Deriv App ID (from developers.deriv.com)
"""

import asyncio
import json
import os
import sys

import aiohttp
import websockets
import pandas as pd
import ta

# ====== CONFIG ======
API_TOKEN     = os.environ.get("DERIV_API_TOKEN")
APP_ID        = os.environ.get("DERIV_APP_ID")

SYMBOLS       = ["R_25", "R_75", "R_100"]   # Volatility 25 / 75 / 100 Indices
GRANULARITY   = 900         # candle size in seconds (900 = 15 minutes)
CANDLE_COUNT  = 110         # fetch extra so we have ~100+ closed candles after
                            # dropping the currently-forming one
STAKE         = 10          # stake per trade in USD
DURATION      = 5
DURATION_UNIT = "m"         # m = minutes
MIN_STREAK    = 5

# --- Price structure filter ---
# A signal is only confirmed if the closing price of the signal candle is
# within STRUCTURE_PROXIMITY_PCT of a recent swing low (for CALL) or swing
# high (for PUT), calculated over the STRUCTURE_LOOKBACK candles preceding
# the signal candle. This avoids taking reversal trades in the middle of a
# range, where MACD/AO can flip without real support or resistance behind it.
STRUCTURE_LOOKBACK       = 25     # candles to look back for swing high/low
STRUCTURE_PROXIMITY_PCT  = 0.5    # must be within this % of the swing level

ACCOUNTS_ENDPOINT = "https://api.derivws.com/trading/v1/options/accounts"
PUBLIC_WS_URL     = "wss://api.derivws.com/trading/v1/options/ws/public"


def get_headers():
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": str(APP_ID),
        "Content-Type": "application/json",
    }


# ====== STEP 1: AUTO-DISCOVER DEMO ACCOUNT ID ======

async def get_demo_account_id() -> str:
    """Call the accounts list endpoint and find the demo Options account ID."""
    async with aiohttp.ClientSession() as session:
        async with session.get(ACCOUNTS_ENDPOINT, headers=get_headers()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Accounts request failed ({resp.status}): {body}")
            data = await resp.json()
            accounts = data.get("data", [])

            if not accounts:
                raise RuntimeError(
                    "No Options accounts found for these credentials. "
                    "Make sure your PAT has the 'trade' scope and belongs to the "
                    "same Deriv login as your demo Options account."
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


# ====== STEP 2: GET FRESH OTP URL (called only when a signal fires) ======

async def get_fresh_otp_url(account_id: str) -> str:
    """
    Request a brand-new one-time WebSocket URL from the OTP endpoint.
    Must be called immediately before connecting — the URL is short-lived
    and single-use, so we never cache it between loop iterations.
    """
    otp_endpoint = (
        f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(otp_endpoint, headers=get_headers()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"OTP request failed ({resp.status}): {body}")
            data = await resp.json()
            otp_url = data["data"]["url"]
            print(f"Fresh OTP URL obtained.")
            return otp_url


# ====== STEP 3: FETCH CLOSED CANDLES (public WS, no auth needed) ======

async def get_closed_candles(symbol: str) -> list:
    """
    Fetch candles for a given symbol from the public WebSocket and drop the
    last one, which is the currently-forming (unclosed) candle.
    Returns only fully closed candles so signals are never based on
    incomplete price data.
    """
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
            response = json.loads(await ws.recv())
            if "error" in response:
                raise RuntimeError(
                    response["error"].get("message", "Unknown Deriv error")
                )
            if response.get("msg_type") == "candles":
                candles = response["candles"]
                # Drop the last candle — it is still forming
                closed = candles[:-1]
                print(f"[{symbol}] Fetched {len(candles)} candles, "
                      f"using {len(closed)} closed.")
                return closed


def build_dataframe(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df["close"] = df["close"].astype(float)
    df["high"]  = df["high"].astype(float)
    df["low"]   = df["low"].astype(float)
    return df


# ====== STEP 4: SIGNAL DETECTION ======

def near_structure_level(df: pd.DataFrame, direction: str) -> bool:
    """
    Check whether the signal candle's close is near a recent swing low
    (direction='CALL', i.e. testing support) or swing high
    (direction='PUT', i.e. testing resistance), using the
    STRUCTURE_LOOKBACK candles immediately preceding the signal candle
    (not including the signal candle itself, so the level isn't
    contaminated by the candle we're judging).
    """
    window = df.iloc[-(STRUCTURE_LOOKBACK + 1):-1]
    if len(window) < STRUCTURE_LOOKBACK:
        return False  # not enough history to establish structure yet

    signal_close = df["close"].iloc[-1]

    if direction == "CALL":
        swing_low = window["low"].min()
        distance_pct = abs(signal_close - swing_low) / swing_low * 100
        return distance_pct <= STRUCTURE_PROXIMITY_PCT
    else:  # PUT
        swing_high = window["high"].max()
        distance_pct = abs(swing_high - signal_close) / swing_high * 100
        return distance_pct <= STRUCTURE_PROXIMITY_PCT


def check_signal(df: pd.DataFrame):
    """
    Evaluate MACD + AO strategy on the most recent CLOSED candle.

    Indexing convention (all indices are into the closed-candle DataFrame):
      signal_candle  = iloc[-1]  -> the most recently closed candle
      prev_candle    = iloc[-2]  -> the candle before that
      streak_end     = iloc[-2]  -> last candle of the preceding same-color run
      streak_start   = iloc[-(MIN_STREAK+1)]  -> earliest candle of that run

    AO streak check: the MIN_STREAK candles BEFORE signal_candle
    (i.e. iloc[-MIN_STREAK-1] through iloc[-2]) must all be the same color,
    and signal_candle (iloc[-1]) must be the opposite color.

    Returns a tuple (signal, candle_epoch) where signal is "CALL"/"PUT"/None
    and candle_epoch is the epoch of the signal candle (for dedup tracking).
    """
    signal_epoch = df.iloc[-1]["epoch"] if "epoch" in df.columns else None

    # --- MACD ---
    macd_ind    = ta.trend.MACD(
        close=df["close"], window_slow=26, window_fast=12, window_sign=9
    )
    macd_line   = macd_ind.macd()
    signal_line = macd_ind.macd_signal()

    # Cross on the signal candle: prev was on one side, last is on the other
    macd_cross_up   = (macd_line.iloc[-2] < signal_line.iloc[-2] and
                       macd_line.iloc[-1] > signal_line.iloc[-1])
    macd_cross_down = (macd_line.iloc[-2] > signal_line.iloc[-2] and
                       macd_line.iloc[-1] < signal_line.iloc[-1])

    # Zone check uses prev_candle to confirm where MACD was before the cross
    below_zero = macd_line.iloc[-2] < 0
    above_zero = macd_line.iloc[-2] > 0

    # --- AO ---
    ao_ind = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    )
    ao = ao_ind.awesome_oscillator()

    # A bar is "green" when its AO value is higher than the previous bar's
    ao_green = ao > ao.shift(1)

    # The signal candle must flip color relative to the candle before it
    ao_flip_to_green = (not ao_green.iloc[-2]) and ao_green.iloc[-1]
    ao_flip_to_red   = ao_green.iloc[-2] and (not ao_green.iloc[-1])

    def had_streak_before_flip(streak_color_is_green: bool) -> bool:
        """
        Check that the MIN_STREAK candles immediately preceding the signal
        candle (iloc[-MIN_STREAK-1] through iloc[-2]) are all streak_color.
        These are the candles BEFORE the flip, not including the flip itself.
        """
        for i in range(-2, -MIN_STREAK - 2, -1):
            if ao_green.iloc[i] != streak_color_is_green:
                return False
        return True

    bullish = (macd_cross_up   and below_zero and
               ao_flip_to_green and had_streak_before_flip(False))
    bearish = (macd_cross_down and above_zero and
               ao_flip_to_red   and had_streak_before_flip(True))

    # --- Price structure filter ---
    # Look at the STRUCTURE_LOOKBACK candles before the signal candle.
    # For a CALL: closing price must be within STRUCTURE_PROXIMITY_PCT of the
    #             recent swing LOW (near support).
    # For a PUT:  closing price must be within STRUCTURE_PROXIMITY_PCT of the
    #             recent swing HIGH (near resistance).
    close_price  = df["close"].iloc[-1]
    lookback_df  = df.iloc[-(STRUCTURE_LOOKBACK + 1):-1]  # exclude signal candle
    swing_low    = lookback_df["low"].min()
    swing_high   = lookback_df["high"].max()

    near_support    = close_price <= swing_low  * (1 + STRUCTURE_PROXIMITY_PCT / 100)
    near_resistance = close_price >= swing_high * (1 - STRUCTURE_PROXIMITY_PCT / 100)

    if bullish and near_support:
        return "CALL", signal_epoch
    elif bullish:
        print(f"  CALL signal found but price ({close_price:.4f}) not near "
              f"support ({swing_low:.4f}), skipping.")

    if bearish and near_resistance:
        return "PUT", signal_epoch
    elif bearish:
        print(f"  PUT signal found but price ({close_price:.4f}) not near "
              f"resistance ({swing_high:.4f}), skipping.")

    return None, signal_epoch


# ====== STEP 5: PLACE TRADE ======

async def place_trade(account_id: str, symbol: str, contract_type: str):
    """
    Request a fresh OTP URL, connect immediately, get a proposal, then buy.
    The OTP is consumed in a single session — never stored between iterations.
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
            msg = json.loads(await ws.recv())
            if msg.get("msg_type") == "proposal":
                if "error" in msg:
                    print(f"[{symbol}] Proposal error:", msg["error"]["message"])
                    return
                proposal_id = msg["proposal"]["id"]
                ask_price   = msg["proposal"]["ask_price"]
                print(f"[{symbol}] Proposal received — ID: {proposal_id}, "
                      f"Ask: {ask_price}")
                break

        buy_request = {
            "buy": proposal_id,
            "price": ask_price,
        }
        await ws.send(json.dumps(buy_request))
        buy_response = json.loads(await ws.recv())
        if "error" in buy_response:
            print(f"[{symbol}] Buy error:", buy_response["error"]["message"])
        else:
            print(f"[{symbol}] Trade placed successfully:", buy_response)


# ====== PER-SYMBOL STRATEGY LOOP ======

async def run_symbol_loop(symbol: str, account_id_holder: dict):
    """
    Independent strategy loop for a single symbol. Runs forever, checking
    for a signal every GRANULARITY seconds. Tracks the epoch of the last
    candle that triggered a trade so the same closed candle is never traded
    twice (duplicate-candle protection).
    """
    last_traded_epoch = None

    while True:
        try:
            candles = await get_closed_candles(symbol)
            df = build_dataframe(candles)

            min_required = MIN_STREAK + 5   # streak candles + MACD warmup buffer
            if len(df) < min_required:
                print(f"[{symbol}] Not enough closed candles yet "
                      f"({len(df)} / {min_required} needed).")
            else:
                signal, signal_epoch = check_signal(df)
                if signal and signal_epoch == last_traded_epoch:
                    print(f"[{symbol}] Signal {signal} already traded for this "
                          f"candle (epoch {signal_epoch}), skipping.")
                elif signal:
                    print(f"[{symbol}] Signal detected: {signal} — "
                          f"requesting fresh OTP and placing trade...")
                    await place_trade(account_id_holder["id"], symbol, signal)
                    last_traded_epoch = signal_epoch
                else:
                    print(f"[{symbol}] No signal on this closed candle.")

        except Exception as e:
            print(f"[{symbol}] Error in strategy loop: {e}")
            # Re-discover account on any failure (e.g. network blip)
            try:
                print(f"[{symbol}] Re-discovering demo account...")
                account_id_holder["id"] = await get_demo_account_id()
            except Exception as refresh_err:
                print(f"[{symbol}] Failed to re-discover account: {refresh_err}")

        await asyncio.sleep(GRANULARITY)


# ====== MAIN ======

async def main():
    missing = [v for v in ("DERIV_API_TOKEN", "DERIV_APP_ID") if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing environment variable(s): {', '.join(missing)}")
        print("Set them in Railway under your service's Variables tab.")
        sys.exit(1)

    print("Discovering demo Options account...")
    account_id = await get_demo_account_id()
    # Shared mutable holder so all symbol loops see account_id refreshes
    account_id_holder = {"id": account_id}

    print(f"Bot started. Running strategy loop on: {', '.join(SYMBOLS)}")

    # Run one independent strategy loop per symbol, concurrently
    await asyncio.gather(*[
        run_symbol_loop(symbol, account_id_holder) for symbol in SYMBOLS
    ])


if __name__ == "__main__":
    asyncio.run(main())
