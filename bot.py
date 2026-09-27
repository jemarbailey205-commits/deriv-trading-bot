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
- Step 1: GET /trading/v1/options/accounts to auto-discover your demo account ID
- Step 2: POST to the OTP endpoint with that account ID to get a one-time WebSocket URL
- Step 3: Connect to that OTP URL to place authenticated trades

Environment variables required (set in Railway → Variables tab):
  DERIV_API_TOKEN   Your Personal Access Token (PAT) from Deriv (needs trade scope)
  DERIV_APP_ID      Your Deriv App ID (from developers.deriv.com)

NOTE: DERIV_ACCOUNT_ID is no longer needed — the bot finds it automatically.
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
GRANULARITY   = 3600        # candle size in seconds (3600 = 1 hour)
CANDLE_COUNT  = 100
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

            # Find the active demo account
            for acc in accounts:
                if acc.get("account_type") == "demo" and acc.get("status") == "active":
                    account_id = acc["account_id"]
                    print(f"Found demo Options account: {account_id} "
                          f"(balance: {acc['balance']} {acc['currency']})")
                    return account_id

            # If no demo found, list what we got to help debugging
            print("Available accounts:")
            for acc in accounts:
                print(f"  {acc.get('account_id')} | type: {acc.get('account_type')} | status: {acc.get('status')}")
            raise RuntimeError("No active demo Options account found.")


# ====== STEP 2: GET OTP URL ======

async def get_otp_url(account_id: str) -> str:
    """Exchange the PAT for a one-time authenticated WebSocket URL."""
    otp_endpoint = f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"
    async with aiohttp.ClientSession() as session:
        async with session.post(otp_endpoint, headers=get_headers()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"OTP request failed ({resp.status}): {body}")
            data = await resp.json()
            otp_url = data["data"]["url"]
            print(f"OTP URL obtained: {otp_url[:60]}...")
            return otp_url


# ====== STEP 3: FETCH CANDLES (public WS, no auth needed) ======

async def get_candles() -> list:
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


# ====== STEP 4: SIGNAL DETECTION ======

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


# ====== STEP 5: PLACE TRADE (authenticated WS) ======

async def place_trade(otp_url: str, contract_type: str):
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

    print("Fetching OTP URL...")
    otp_url = await get_otp_url(account_id)

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
            try:
                print("Re-discovering account and refreshing OTP URL...")
                account_id = await get_demo_account_id()
                otp_url = await get_otp_url(account_id)
            except Exception as refresh_err:
                print(f"Failed to refresh: {refresh_err}")

        await asyncio.sleep(GRANULARITY)


if __name__ == "__main__":
    asyncio.run(main())
