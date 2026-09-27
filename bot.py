"""
Deriv MACD + Awesome Oscillator Reversal Bot  (API v1)
-------------------------------------------------------
Strategy:
- MACD(12, 26, 9): the MACD line crossing the signal line while BELOW the
  zero line signals a possible bullish reversal; crossing while ABOVE zero
  signals a possible bearish reversal.
- Awesome Oscillator (AO): at least MIN_STREAK candles of the same color
  (preceding the flip candle), then a color flip on the signal candle confirms
  momentum has actually turned.
- A trade is only placed when both conditions align on the same CLOSED candle.

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

SYMBOL        = "R_75"      # Volatility 75 Index
GRANULARITY   = 300         # candle size in seconds (300 = 5 minutes)
CANDLE_COUNT  = 110         # fetch extra so we have 100 closed candles after
                            # dropping the currently-forming one
STAKE         = 10          # stake per trade in USD
DURATION      = 5
DURATION_UNIT = "m"         # m = minutes
MIN_STREAK    = 5

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

async def get_closed_candles() -> list:
    """
    Fetch candles from the public WebSocket and drop the last one,
    which is the currently-forming (unclosed) candle.
    Returns only fully closed candles so signals are never based on
    incomplete price data.
    """
    request = {
        "ticks_history": SYMBOL,
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
            if response.get("msg_type") == "candles":
                candles = response["candles"]
                # Drop the last candle — it is still forming
                closed = candles[:-1]
                print(f"Fetched {len(candles)} candles, using {len(closed)} closed.")
                return closed


def build_dataframe(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df["close"] = df["close"].astype(float)
    df["high"]  = df["high"].astype(float)
    df["low"]   = df["low"].astype(float)
    return df


# ====== STEP 4: SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    """
    Evaluate MACD + AO strategy on the most recent CLOSED candle.

    Indexing convention (all indices are into the closed-candle DataFrame):
      signal_candle  = iloc[-1]  → the most recently closed candle
      prev_candle    = iloc[-2]  → the candle before that
      streak_end     = iloc[-2]  → last candle of the preceding same-color run
      streak_start   = iloc[-(MIN_STREAK+1)]  → earliest candle of that run

    AO streak check: the MIN_STREAK candles BEFORE signal_candle
    (i.e. iloc[-MIN_STREAK-1] through iloc[-2]) must all be the same color,
    and signal_candle (iloc[-1]) must be the opposite color.
    """
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

    if bullish:
        return "CALL"
    if bearish:
        return "PUT"
    return None


# ====== STEP 5: PLACE TRADE ======

async def place_trade(account_id: str, contract_type: str):
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
        "underlying_symbol": SYMBOL,
    }

    async with websockets.connect(otp_url) as ws:
        await ws.send(json.dumps(proposal_request))
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("msg_type") == "proposal":
                if "error" in msg:
                    print("Proposal error:", msg["error"]["message"])
                    return
                proposal_id = msg["proposal"]["id"]
                ask_price   = msg["proposal"]["ask_price"]
                print(f"Proposal received — ID: {proposal_id}, Ask: {ask_price}")
                break

        buy_request = {
            "buy": proposal_id,
            "price": ask_price,
        }
        await ws.send(json.dumps(buy_request))
        buy_response = json.loads(await ws.recv())
        if "error" in buy_response:
            print("Buy error:", buy_response["error"]["message"])
        else:
            print("Trade placed successfully:", buy_response)


# ====== MAIN LOOP ======

async def main():
    missing = [v for v in ("DERIV_API_TOKEN", "DERIV_APP_ID") if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing environment variable(s): {', '.join(missing)}")
        print("Set them in Railway under your service's Variables tab.")
        sys.exit(1)

    print("Discovering demo Options account...")
    account_id = await get_demo_account_id()
    print("Bot started. Running strategy loop...")

    while True:
        try:
            candles = await get_closed_candles()
            df = build_dataframe(candles)

            min_required = MIN_STREAK + 5   # streak candles + MACD warmup buffer
            if len(df) < min_required:
                print(f"Not enough closed candles yet "
                      f"({len(df)} / {min_required} needed).")
            else:
                signal = check_signal(df)
                if signal:
                    print(f"Signal detected: {signal} — requesting fresh OTP and placing trade...")
                    await place_trade(account_id, signal)
                else:
                    print("No signal on this closed candle.")

        except Exception as e:
            print(f"Error in main loop: {e}")
            # Re-discover account on any failure (e.g. network blip)
            try:
                print("Re-discovering demo account...")
                account_id = await get_demo_account_id()
            except Exception as refresh_err:
                print(f"Failed to re-discover account: {refresh_err}")

        await asyncio.sleep(GRANULARITY)


if __name__ == "__main__":
    asyncio.run(main())
