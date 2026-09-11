"""
Deriv MACD + Awesome Oscillator Reversal Bot
----------------------------------------------
Strategy:
- MACD(12, 26, 9): the MACD line crossing the signal line while BELOW the
  zero line signals a possible bullish reversal; crossing while ABOVE zero
  signals a possible bearish reversal.
- Awesome Oscillator (AO): at least MIN_STREAK candles of one color, then a
  flip to the opposite color confirms momentum has actually turned.
- A trade is only placed when both the MACD condition and the AO flip agree
  on the same closed candle.

This trades on Deriv's synthetic indices / forex via the official
WebSocket API. It uses your Deriv API token (from Settings -> API Token in
your Deriv account), NOT your email and password.
"""

import asyncio
import json
import os
import websockets
import pandas as pd
import ta

# ====== CONFIG ======
API_TOKEN = os.environ.get("DERIV_API_TOKEN", "pat_3d4176dca9da8e3eeebbc2ef303e91725c0005a721f675e5287acfb67245a953)
APP_ID = 1089  # Deriv's default public app_id, fine for personal bots
SYMBOL = "R_75"          # e.g. Volatility 75 Index. Change to "frxEURUSD" etc for forex
GRANULARITY = 3600        # candle size in seconds (3600 = 1 hour, 86400 = 1 day)
CANDLE_COUNT = 100        # how many historical candles to keep in memory
STAKE = 10                # stake per trade in account currency
DURATION = 5
DURATION_UNIT = "m"       # duration unit for the contract (m = minutes)
MIN_STREAK = 5            # minimum consecutive same-colour AO bars before a flip counts

DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"


async def send_and_wait(ws, request):
    await ws.send(json.dumps(request))
    while True:
        response = json.loads(await ws.recv())
        if response.get("echo_req", {}).get(request.get("ticks_history", request.get("authorize", request.get("buy", "")))) is not None:
            pass
        return response


async def get_candles(ws):
    request = {
        "ticks_history": SYMBOL,
        "adjust_start_time": 1,
        "count": CANDLE_COUNT,
        "end": "latest",
        "granularity": GRANULARITY,
        "style": "candles",
    }
    await ws.send(json.dumps(request))
    while True:
        response = json.loads(await ws.recv())
        if response.get("msg_type") == "candles":
            return response["candles"]


def build_dataframe(candles):
    df = pd.DataFrame(candles)
    df["close"] = df["close"].astype(float)
    return df


def check_signal(df):
    macd_indicator = ta.trend.MACD(
        close=df["close"], window_slow=26, window_fast=12, window_sign=9
    )
    macd_line = macd_indicator.macd()
    signal_line = macd_indicator.macd_signal()

    ao_indicator = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"].astype(float), low=df["low"].astype(float)
    )
    ao = ao_indicator.awesome_oscillator()

    # AO "green" = higher than previous bar, "red" = lower than previous bar
    ao_green = ao > ao.shift(1)

    last = -1
    prev = -2

    macd_cross_up = macd_line.iloc[prev] < signal_line.iloc[prev] and macd_line.iloc[last] > signal_line.iloc[last]
    macd_cross_down = macd_line.iloc[prev] > signal_line.iloc[prev] and macd_line.iloc[last] < signal_line.iloc[last]
    below_zero = macd_line.iloc[prev] < 0
    above_zero = macd_line.iloc[prev] > 0

    ao_flip_to_green = (not ao_green.iloc[prev]) and ao_green.iloc[last]
    ao_flip_to_red = ao_green.iloc[prev] and (not ao_green.iloc[last])

    def had_streak(is_green, start_index):
        count = 0
        for i in range(start_index, start_index - MIN_STREAK, -1):
            if ao_green.iloc[i] == is_green:
                count += 1
            else:
                break
        return count >= MIN_STREAK

    bullish = macd_cross_up and below_zero and ao_flip_to_green and had_streak(False, prev)
    bearish = macd_cross_down and above_zero and ao_flip_to_red and had_streak(True, prev)

    if bullish:
        return "CALL"
    if bearish:
        return "PUT"
    return None


async def place_trade(ws, contract_type):
    proposal_request = {
        "buy": 1,
        "price": STAKE,
        "parameters": {
            "amount": STAKE,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "duration": DURATION,
            "duration_unit": DURATION_UNIT,
            "symbol": SYMBOL,
        },
    }
    await ws.send(json.dumps(proposal_request))
    response = json.loads(await ws.recv())
    print("Trade response:", response)


async def main():
    async with websockets.connect(DERIV_WS_URL) as ws:
        await ws.send(json.dumps({"authorize": API_TOKEN}))
        auth_response = json.loads(await ws.recv())
        if "error" in auth_response:
            print("Authorization failed:", auth_response["error"]["message"])
            return
        print("Connected as:", auth_response["authorize"]["loginid"])

        while True:
            candles = await get_candles(ws)
            df = build_dataframe(candles)

            if len(df) >= MIN_STREAK + 3:
                signal = check_signal(df)
                if signal:
                    print(f"Signal detected: {signal}")
                    await place_trade(ws, signal)
                else:
                    print("No signal this candle.")

            await asyncio.sleep(GRANULARITY)


if __name__ == "__main__":
    asyncio.run(main())
