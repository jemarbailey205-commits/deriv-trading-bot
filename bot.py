"""
Deriv AO + AC Breakout Bot  (API v1)
-------------------------------------------------------
Strategy (run independently per symbol):
- Structure break: the signal candle's close must actually break outside
  the recent swing high (bullish) or swing low (bearish), measured over
  the STRUCTURE_LOOKBACK candles BEFORE the signal candle. This is a real
  break, not just proximity to the level.
- Momentum confirmation: AO or AC (at least one) must have crossed the zero
  line in the same direction as the break, either on the signal candle itself
  or up to ZERO_CROSS_WINDOW candles before it. The indicator that crossed
  must still be on the correct side of zero on the signal candle.
- A trade is only placed when both conditions align on the same CLOSED candle.

Timeframe: 15-minute candles.

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

import aiohttp
import websockets
import pandas as pd
import ta

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
AC_SMA_PERIOD     = 5        # AC = AO - SMA(AO, 5)  (standard Bill Williams)
ZERO_CROSS_WINDOW = 2        # zero cross may be on the signal candle or up to
                             # this many candles before it
MIN_CANDLES       = 50       # AO needs 34, AC needs ~38; extra buffer

# --- Structure filter ---
STRUCTURE_LOOKBACK = 25      # candles to look back for swing high/low

# --- Timing / robustness ---
CANDLE_CLOSE_DELAY = 2       # seconds after the boundary before checking
WS_TIMEOUT         = 15      # seconds to wait for any WebSocket reply

ACCOUNTS_ENDPOINT = "https://api.derivws.com/trading/v1/options/accounts"
PUBLIC_WS_URL     = "wss://api.derivws.com/trading/v1/options/ws/public"


def get_headers():
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": str(APP_ID),
        "Content-Type": "application/json",
    }


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


# ====== INDICATOR HELPERS ======

def crossed_up_within(series: pd.Series, window: int) -> bool:
    """
    True if the series crossed from <= 0 to > 0 on the signal candle or on
    any of the `window` candles before it, AND is still above zero on the
    signal candle (so a cross that has already reversed doesn't count).
    """
    if not series.iloc[-1] > 0:
        return False
    for k in range(window + 1):
        idx = -1 - k
        if series.iloc[idx - 1] <= 0 < series.iloc[idx]:
            return True
    return False


def crossed_down_within(series: pd.Series, window: int) -> bool:
    """
    True if the series crossed from >= 0 to < 0 on the signal candle or on
    any of the `window` candles before it, AND is still below zero on the
    signal candle.
    """
    if not series.iloc[-1] < 0:
        return False
    for k in range(window + 1):
        idx = -1 - k
        if series.iloc[idx - 1] >= 0 > series.iloc[idx]:
            return True
    return False


def broke_resistance(df: pd.DataFrame) -> bool:
    """Signal candle closes ABOVE the swing high of the prior lookback window."""
    window = df.iloc[-(STRUCTURE_LOOKBACK + 1):-1]
    if len(window) < STRUCTURE_LOOKBACK:
        return False
    return df["close"].iloc[-1] > window["high"].max()


def broke_support(df: pd.DataFrame) -> bool:
    """Signal candle closes BELOW the swing low of the prior lookback window."""
    window = df.iloc[-(STRUCTURE_LOOKBACK + 1):-1]
    if len(window) < STRUCTURE_LOOKBACK:
        return False
    return df["close"].iloc[-1] < window["low"].min()


# ====== SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    """
    Returns (signal, candle_epoch): signal is "CALL" / "PUT" / None.

    CALL: close breaks above the recent swing high AND AO or AC crossed up
          through zero on the signal candle or within the previous
          ZERO_CROSS_WINDOW candles (and is still above zero).
    PUT:  close breaks below the recent swing low AND AO or AC crossed down
          through zero on the signal candle or within the previous
          ZERO_CROSS_WINDOW candles (and is still below zero).
    """
    signal_epoch = df.iloc[-1]["epoch"] if "epoch" in df.columns else None

    ao = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    ).awesome_oscillator()
    ac = ao - ao.rolling(AC_SMA_PERIOD).mean()

    momentum_up = (crossed_up_within(ao, ZERO_CROSS_WINDOW) or
                   crossed_up_within(ac, ZERO_CROSS_WINDOW))
    momentum_down = (crossed_down_within(ao, ZERO_CROSS_WINDOW) or
                     crossed_down_within(ac, ZERO_CROSS_WINDOW))

    close_price = df["close"].iloc[-1]

    if momentum_up and broke_resistance(df):
        return "CALL", signal_epoch
    if momentum_up:
        print(f"  Zero-cross up seen but price ({close_price:.4f}) hasn't "
              f"broken resistance, skipping.")

    if momentum_down and broke_support(df):
        return "PUT", signal_epoch
    if momentum_down:
        print(f"  Zero-cross down seen but price ({close_price:.4f}) hasn't "
              f"broken support, skipping.")

    return None, signal_epoch


# ====== PLACE TRADE ======

async def place_trade(account_id: str, symbol: str, contract_type: str) -> bool:
    """Returns True only if the buy succeeded."""
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
                    print(f"[{symbol}] Proposal error:", msg["error"]["message"])
                    return False
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
            print(f"[{symbol}] Buy error:", buy_response["error"]["message"])
            return False
        print(f"[{symbol}] Trade placed successfully:", buy_response)
        return True


# ====== PER-SYMBOL LOOP ======

async def sleep_until_next_candle():
    now = time.time()
    next_boundary = (int(now // GRANULARITY) + 1) * GRANULARITY
    await asyncio.sleep(next_boundary - now + CANDLE_CLOSE_DELAY)


async def run_symbol_loop(symbol: str, account_id_holder: dict):
    last_traded_epoch = None

    while True:
        try:
            candles = await get_closed_candles(symbol)
            df = build_dataframe(candles)

            if len(df) < MIN_CANDLES:
                print(f"[{symbol}] Not enough closed candles "
                      f"({len(df)} / {MIN_CANDLES} needed).")
            else:
                signal, signal_epoch = check_signal(df)
                if signal and signal_epoch == last_traded_epoch:
                    print(f"[{symbol}] Signal {signal} already traded for "
                          f"epoch {signal_epoch}, skipping.")
                elif signal:
                    print(f"[{symbol}] Signal detected: {signal} - placing trade...")
                    try:
                        ok = await place_trade(
                            account_id_holder["id"], symbol, signal
                        )
                        if ok:
                            last_traded_epoch = signal_epoch
                    except Exception as trade_err:
                        print(f"[{symbol}] Trade error: {trade_err}")
                        try:
                            account_id_holder["id"] = await get_demo_account_id()
                        except Exception as refresh_err:
                            print(f"[{symbol}] Account re-discovery failed: "
                                  f"{refresh_err}")
                else:
                    print(f"[{symbol}] No signal on this closed candle.")

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

    await asyncio.gather(*[
        run_symbol_loop(symbol, account_id_holder) for symbol in SYMBOLS
    ])


if __name__ == "__main__":
    asyncio.run(main())
