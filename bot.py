"""
Deriv MACD + Awesome Oscillator Reversal Bot  (API v1)
-------------------------------------------------------
Strategy:
- MACD(12, 26, 9): the MACD line crossing the signal line while BELOW the
  zero line signals a possible bullish reversal; crossing while ABOVE zero
  signals a possible bearish reversal.
- Awesome Oscillator (AO): at least MIN_STREAK candles of one color, then a
  flip to the opposite color confirms momentum has actually turned.
- A trade is only placed when both the MACD condition and the AO flip agree
  on the same closed candle.

Authentication (new Deriv API v1):
- Step 1: POST to the OTP endpoint with your Bearer token to get a one-time
  WebSocket URL.
- Step 2: Connect to that OTP URL to place authenticated trades.
- Public market data (candles) uses the public WebSocket — no auth needed.

Environment variables required (set in Railway → Variables tab):
  DERIV_API_TOKEN   Your Personal Access Token (PAT) from Deriv
  DERIV_APP_ID      Your Deriv App ID (create one at developers.deriv.com)
  DERIV_ACCOUNT_ID  Your Deriv account ID (e.g. CR123456)

IMPORTANT: Never hardcode tokens in this file — this repo is public on GitHub.
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
API_TOKEN    = os.environ.get("DERIV_API_TOKEN")    # Personal Access Token (PAT)
APP_ID       = os.environ.get("DERIV_APP_ID")       # Your Deriv App ID
ACCOUNT_ID   = os.environ.get("DERIV_ACCOUNT_ID")   # e.g. CR123456

SYMBOL       = "R_75"      # Volatility 75 Index. Change to e.g. "frxEURUSD" for forex
GRANULARITY  = 3600        # candle size in seconds (3600 = 1 hour)
CANDLE_COUNT = 100         # how many historical candles to keep in memory
STAKE        = 10          # stake per trade in USD
DURATION     = 5
DURATION_UNIT = "m"        # m = minutes, s = seconds, h = hours, d = days, t = ticks
MIN_STREAK   = 5           # minimum consecutive same-colour AO bars before a flip counts

# Deriv API v1 endpoints
OTP_ENDPOINT   = f"https://api.derivws.com/trading/v1/options/accounts/{ACCOUNT_ID}/otp"
PUBLIC_WS_URL  = "wss://api.derivws.com/trading/v1/options/ws/public"


# ====== STEP 1: GET OTP URL ======

async def get_otp_url() -> str:
    """Exchange the PAT for a one-time authenticated WebSocket URL."""
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": APP_ID,
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(OTP_ENDPOINT, headers=headers) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"OTP request failed ({resp.status}): {body}")
            data = await resp.json()
            otp_url = data["data"]["url"]
            print(f"OTP URL obtained: {otp_url[:60]}...")
            return otp_url


# ====== STEP 2: FETCH CANDLES (public WS, no auth needed) ======

async def get_candles() -> list:
    """Fetch historical candles from the public WebSocket endpoint."""
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
                return response["candles"]


def build_dataframe(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df["close"] = df["close"].astype(float)
    df["high"]  = df["high"].astype(float)
    df["low"]   = df["low"].astype(float)
    return df


# ====== STEP 3: SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    macd_indicator = ta.trend.MACD(
        close=df["close"], window_slow=26, window_fast=12, window_sign=9
    )
    macd_line   = macd_indicator.macd()
    signal_line = macd_indicator.macd_signal()

    ao_indicator = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    )
    ao = ao_indicator.awesome_oscillator()

    # AO "green" = higher than previous bar, "red" = lower
    ao_green = ao > ao.shift(1)

    last = -1
    prev = -2

    macd_cross_up   = macd_line.iloc[prev] < signal_line.iloc[prev] and macd_line.iloc[last] > signal_line.iloc[last]
    macd_cross_down = macd_line.iloc[prev] > signal_line.iloc[prev] and macd_line.iloc[last] < signal_line.iloc[last]
    below_zero = macd_line.iloc[prev] < 0
    above_zero = macd_line.iloc[prev] > 0

    ao_flip_to_green = (not ao_green.iloc[prev]) and ao_green.iloc[last]
    ao_flip_to_red   = ao_green.iloc[prev] and (not ao_green.iloc[last])

    def had_streak(is_green: bool, start_index: int) -> bool:
        count = 0
        for i in range(start_index, start_index - MIN_STREAK, -1):
            if ao_green.iloc[i] == is_green:
                count += 1
            else:
                break
        return count >= MIN_STREAK

    bullish = macd_cross_up   and below_zero and ao_flip_to_green and had_streak(False, prev)
    bearish = macd_cross_down and above_zero and ao_flip_to_red   and had_streak(True,  prev)

    if bullish:
        return "CALL"
    if bearish:
        return "PUT"
    return None


# ====== STEP 4: PLACE TRADE (authenticated WS) ======

async def place_trade(otp_url: str, contract_type: str):
    """
    Correct Deriv v1 trade flow:
      1. Connect to the OTP WebSocket URL.
      2. Send a proposal request — get back a proposal ID and ask_price.
      3. Send a buy request using that proposal ID and ask_price.
    """
    proposal_request = {
        "proposal": 1,
        "amount": STAKE,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": "USD",
        "duration": DURATION,
        "duration_unit": DURATION_UNIT,
        "underlying_symbol": SYMBOL,   # new field name in v1 API
    }

    async with websockets.connect(otp_url) as ws:
        # --- Get proposal ---
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

        # --- Buy using proposal ID ---
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
    # Validate environment variables
    missing = [v for v in ("DERIV_API_TOKEN", "DERIV_APP_ID", "DERIV_ACCOUNT_ID") if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing environment variable(s): {', '.join(missing)}")
        print("Set them in Railway under your service's Variables tab.")
        sys.exit(1)

    print("Fetching OTP URL...")
    otp_url = await get_otp_url()
    print("Bot started. Running strategy loop...")

    while True:
        try:
            candles = await get_candles()
            df = build_dataframe(candles)

            if len(df) >= MIN_STREAK + 3:
                signal = check_signal(df)
                if signal:
                    print(f"Signal detected: {signal} — placing trade...")
                    await place_trade(otp_url, signal)
                else:
                    print("No signal this candle.")
            else:
                print(f"Not enough candles yet ({len(df)} / {MIN_STREAK + 3} needed).")

        except Exception as e:
            print(f"Error in main loop: {e}")
            # Re-fetch OTP URL on error in case the session expired
            try:
                print("Re-fetching OTP URL after error...")
                otp_url = await get_otp_url()
            except Exception as otp_err:
                print(f"Failed to refresh OTP URL: {otp_err}")

        await asyncio.sleep(GRANULARITY)


if __name__ == "__main__":
    asyncio.run(main())
