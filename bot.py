"""
Deriv AO + AC Breakout Bot  (API v1)
-------------------------------------------------------
Strategy (run independently per symbol):
- Trigger: AO or AC (at least one) crosses the zero line on the signal candle.
- Confirmation: AO and AC both show the SAME color on the signal candle
  (both green = bullish, both red = bearish), and their color flips to that
  color happened within FLIP_WINDOW candles of each other.
- Structure: the signal candle's close must be within STRUCTURE_PROXIMITY_PCT
  of a recent swing high/low. This is a proximity check, not a hard break.
- All three must align on the same CLOSED candle.

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
GRANULARITY   = 300          # 5-minute candles
CANDLE_COUNT  = 110
STAKE         = 10
DURATION      = 5
DURATION_UNIT = "m"

# --- AO / AC settings ---
AC_SMA_PERIOD = 5            # AC = AO - SMA(AO, 5)  (standard Bill Williams)
FLIP_WINDOW   = 2            # AO and AC color flips must be within this many candles
MIN_CANDLES   = 50           # AO needs 34, AC needs ~38; extra buffer

# --- Structure filter ---
STRUCTURE_LOOKBACK      = 25
STRUCTURE_PROXIMITY_PCT = 1.0
# True  = breakout: bullish signals near the swing HIGH, bearish near the swing LOW
# False = reversal: bullish signals near the swing LOW, bearish near the swing HIGH
BREAKOUT_MODE = True

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

def color_state(series: pd.Series):
    """
    Returns (is_green, age) for the latest bar of an AO/AC series.
    A bar is green when its value is higher than the previous bar's.
    age = how many candles ago the current color started
          (0 = the color flipped on the signal candle itself).
    """
    vals = series.dropna().tolist()
    colors = [vals[i] > vals[i - 1] for i in range(1, len(vals))]
    current = colors[-1]
    run = 0
    for c in reversed(colors):
        if c == current:
            run += 1
        else:
            break
    return current, run - 1


def crossed_up(series: pd.Series) -> bool:
    return series.iloc[-2] <= 0 < series.iloc[-1]


def crossed_down(series: pd.Series) -> bool:
    return series.iloc[-2] >= 0 > series.iloc[-1]


def near_structure_level(df: pd.DataFrame, level_type: str) -> bool:
    """
    True if the signal candle's close is within STRUCTURE_PROXIMITY_PCT of the
    recent swing high ("resistance") or swing low ("support"), measured over
    the STRUCTURE_LOOKBACK candles BEFORE the signal candle. Price may be on
    either side of the level (a break through it still counts as "near").
    """
    window = df.iloc[-(STRUCTURE_LOOKBACK + 1):-1]
    if len(window) < STRUCTURE_LOOKBACK:
        return False

    close = df["close"].iloc[-1]
    if level_type == "support":
        level = window["low"].min()
    else:
        level = window["high"].max()

    distance_pct = abs(close - level) / level * 100
    return distance_pct <= STRUCTURE_PROXIMITY_PCT


# ====== SIGNAL DETECTION ======

def check_signal(df: pd.DataFrame):
    """
    Returns (signal, candle_epoch): signal is "CALL" / "PUT" / None.
    """
    signal_epoch = df.iloc[-1]["epoch"] if "epoch" in df.columns else None

    ao = ta.momentum.AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    ).awesome_oscillator()
    ac = ao - ao.rolling(AC_SMA_PERIOD).mean()

    ao_green, ao_age = color_state(ao)
    ac_green, ac_age = color_state(ac)

    same_color  = ao_green == ac_green
    flips_close = abs(ao_age - ac_age) <= FLIP_WINDOW

    if not (same_color and flips_close):
        return None, signal_epoch

    bullish = ao_green and (crossed_up(ao) or crossed_up(ac))
    bearish = (not ao_green) and (crossed_down(ao) or crossed_down(ac))

    close_price = df["close"].iloc[-1]

    if bullish:
        level_type = "resistance" if BREAKOUT_MODE else "support"
        if near_structure_level(df, level_type):
            return "CALL", signal_epoch
        print(f"  CALL signal found but price ({close_price:.4f}) not near "
              f"{level_type}, skipping.")

    if bearish:
        level_type = "support" if BREAKOUT_MODE else "resistance"
        if near_structure_level(df, level_type):
            return "PUT", signal_epoch
        print(f"  PUT signal found but price ({close_price:.4f}) not near "
              f"{level_type}, skipping.")

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
        buy_response = json.loads(await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT))
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
                        ok = await place_trade(account_id_holder["id"], symbol, signal)
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

    mode = "breakout" if BREAKOUT_MODE else "reversal"
    print(f"Bot started ({mode} mode). Running on: {', '.join(SYMBOLS)}")

    await asyncio.gather(*[
        run_symbol_loop(symbol, account_id_holder) for symbol in SYMBOLS
    ])


if __name__ == "__main__":
    asyncio.run(main())
