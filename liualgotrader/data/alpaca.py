import asyncio
import concurrent.futures
import queue
import time
import traceback
from datetime import date, datetime, timedelta
from random import randint
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandas_market_calendars
import pytz
import requests
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.live.crypto import CryptoDataStream
from alpaca.data.live.stock import StockDataStream
from alpaca.data.requests import (CryptoBarsRequest, StockBarsRequest,
                                  StockLatestTradeRequest,
                                  StockSnapshotRequest)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest
from dateutil.parser import parse as date_parser

from liualgotrader.common import config
from liualgotrader.common.list_utils import chunks
from liualgotrader.common.tlog import tlog, tlog_exception
from liualgotrader.common.types import QueueMapper, TimeScale, WSEventType
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.streaming_base import StreamingAPI

NY = "America/New_York"
nytz = pytz.timezone(NY)


def _is_crypto_symbol(symbol: str) -> bool:
    return symbol.lower() in {"eth/usd", "btc/usd", "ethusd", "btcusd"}


class AlpacaData(DataAPI):
    def __init__(self):
        api_key = config.alpaca_api_key or "PKDUMMYKEY00000000000"
        secret_key = config.alpaca_api_secret or "SKDUMMYSECRET000000000000000000000000"
        self.trading_client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=True,
        )
        self.stock_historical_client = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key,
        )
        self.crypto_historical_client = CryptoHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key,
        )
        # for requesting market snapshots by chunk of symbols
        self.symbol_chunk_size = 1000
        self.datetime_cache: Dict[datetime, datetime] = {}

    def get_symbols(self) -> List[str]:
        req = GetAssetsRequest(
            status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY
        )
        assets = self.trading_client.get_all_assets(req)
        return [asset.symbol for asset in assets if asset.tradable]

    async def get_market_snapshot(
        self, filter_func: Optional[Callable] = None
    ) -> List[Dict]:
        # parse market snapshots per chunk of symbols
        symbols = self.get_symbols()
        return await self._get_symbols_snapshot(symbols, filter_func)

    async def _get_symbols_snapshot(
        self, symbols: List[str], filter_func: Optional[Callable]
    ) -> List[Dict]:
        def _parse_ticker_snapshot(_ticker: str, _snapshot) -> Optional[Dict]:
            try:
                if not _snapshot:
                    return None
                res = {
                    "ticker": _ticker,
                    "latest_trade": {
                        "p": _snapshot.latest_trade.price
                        if _snapshot.latest_trade
                        else None,
                        "s": _snapshot.latest_trade.size
                        if _snapshot.latest_trade
                        else None,
                        "t": _snapshot.latest_trade.timestamp
                        if _snapshot.latest_trade
                        else None,
                        "x": getattr(_snapshot.latest_trade, "exchange", None)
                        if _snapshot.latest_trade
                        else None,
                    }
                    if _snapshot.latest_trade
                    else None,
                    "latest_quote": {
                        "ap": _snapshot.latest_quote.ask_price
                        if _snapshot.latest_quote
                        else None,
                        "as": _snapshot.latest_quote.ask_size
                        if _snapshot.latest_quote
                        else None,
                        "bp": _snapshot.latest_quote.bid_price
                        if _snapshot.latest_quote
                        else None,
                        "bs": _snapshot.latest_quote.bid_size
                        if _snapshot.latest_quote
                        else None,
                        "t": _snapshot.latest_quote.timestamp
                        if _snapshot.latest_quote
                        else None,
                    }
                    if _snapshot.latest_quote
                    else None,
                    "minute_bar": {
                        "o": _snapshot.minute_bar.open
                        if _snapshot.minute_bar
                        else None,
                        "h": _snapshot.minute_bar.high
                        if _snapshot.minute_bar
                        else None,
                        "l": _snapshot.minute_bar.low
                        if _snapshot.minute_bar
                        else None,
                        "c": _snapshot.minute_bar.close
                        if _snapshot.minute_bar
                        else None,
                        "v": _snapshot.minute_bar.volume
                        if _snapshot.minute_bar
                        else None,
                        "vw": _snapshot.minute_bar.vwap
                        if _snapshot.minute_bar
                        else None,
                        "n": _snapshot.minute_bar.trade_count
                        if _snapshot.minute_bar
                        else None,
                        "t": _snapshot.minute_bar.timestamp
                        if _snapshot.minute_bar
                        else None,
                    }
                    if _snapshot.minute_bar
                    else None,
                    "daily_bar": {
                        "o": _snapshot.daily_bar.open
                        if _snapshot.daily_bar
                        else None,
                        "h": _snapshot.daily_bar.high
                        if _snapshot.daily_bar
                        else None,
                        "l": _snapshot.daily_bar.low
                        if _snapshot.daily_bar
                        else None,
                        "c": _snapshot.daily_bar.close
                        if _snapshot.daily_bar
                        else None,
                        "v": _snapshot.daily_bar.volume
                        if _snapshot.daily_bar
                        else None,
                        "vw": _snapshot.daily_bar.vwap
                        if _snapshot.daily_bar
                        else None,
                        "n": _snapshot.daily_bar.trade_count
                        if _snapshot.daily_bar
                        else None,
                        "t": _snapshot.daily_bar.timestamp
                        if _snapshot.daily_bar
                        else None,
                    }
                    if _snapshot.daily_bar
                    else None,
                    "prev_daily_bar": {
                        "o": _snapshot.previous_daily_bar.open
                        if _snapshot.previous_daily_bar
                        else None,
                        "h": _snapshot.previous_daily_bar.high
                        if _snapshot.previous_daily_bar
                        else None,
                        "l": _snapshot.previous_daily_bar.low
                        if _snapshot.previous_daily_bar
                        else None,
                        "c": _snapshot.previous_daily_bar.close
                        if _snapshot.previous_daily_bar
                        else None,
                        "v": _snapshot.previous_daily_bar.volume
                        if _snapshot.previous_daily_bar
                        else None,
                        "vw": _snapshot.previous_daily_bar.vwap
                        if _snapshot.previous_daily_bar
                        else None,
                        "n": _snapshot.previous_daily_bar.trade_count
                        if _snapshot.previous_daily_bar
                        else None,
                        "t": _snapshot.previous_daily_bar.timestamp
                        if _snapshot.previous_daily_bar
                        else None,
                    }
                    if _snapshot.previous_daily_bar
                    else None,
                }
                if (
                    res["latest_trade"] is None
                    or res["daily_bar"] is None
                    or res["prev_daily_bar"] is None
                ):
                    return None
                return res
            except Exception:
                return None

        def _parse_snapshot_and_filter(_symbols: List[str]) -> List[Dict]:
            req = StockSnapshotRequest(symbol_or_symbols=_symbols)
            snapshots = self.stock_historical_client.get_stock_snapshot(req)
            processed_tickers_snapshot = [
                _parse_ticker_snapshot(k, v) for k, v in snapshots.items()
            ]
            return [
                s
                for s in processed_tickers_snapshot
                if s is not None
                and (filter_func(s) if filter_func is not None else True)
            ]

        # request snapshots per chunk of tickers by concurrency
        with concurrent.futures.ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    _parse_snapshot_and_filter,
                    symbols[symbol_idx : symbol_idx + self.symbol_chunk_size],
                )
                for symbol_idx in range(
                    0, len(symbols), self.symbol_chunk_size
                )
            ]

            market_snapshots = [
                y for x in await asyncio.gather(*futures) for y in x
            ]

        return market_snapshots

    def _localize_start_end(self, start: date, end: date) -> Tuple[str, str]:
        return (
            nytz.localize(
                datetime.combine(start, datetime.min.time())
            ).isoformat(),
            (
                nytz.localize(
                    datetime.now().replace(microsecond=0)
                ).isoformat()
                if end >= date.today()
                else nytz.localize(
                    datetime.combine(end, datetime.min.time())
                ).isoformat()
            ),
        )

    def get_last_trading(self, symbol: str) -> datetime:
        if _is_crypto_symbol(symbol):
            return datetime.now(tz=nytz)
        try:
            req = StockLatestTradeRequest(symbol_or_symbols=symbol)
            trade_data = self.stock_historical_client.get_stock_latest_trade(
                req
            )
            if isinstance(trade_data, dict) and symbol in trade_data:
                trade = trade_data[symbol]
            else:
                trade = trade_data
            if not trade or not hasattr(trade, "timestamp"):
                raise ValueError(f"Can't get snapshot for {symbol}")
            return trade.timestamp
        except Exception as e:
            raise ValueError(f"{symbol} snapshot not found") from e

    def get_trading_holidays(self) -> List[str]:
        nyse = pandas_market_calendars.get_calendar("NYSE")
        return nyse.holidays().holidays

    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        if _is_crypto_symbol(symbol):
            cbd_offset = timedelta(days=offset)
        else:
            cbd_offset = pd.tseries.offsets.CustomBusinessDay(
                n=offset - 1, holidays=self.get_trading_holidays()
            )
        return (
            nytz.localize(now + cbd_offset)
            if now.tzinfo is None
            else now + cbd_offset
        )

    def num_trading_minutes(self, symbol: str, start: date, end: date) -> int:
        return (24 if _is_crypto_symbol(symbol) else (20 - 4)) * 60

    def num_trading_days(self, symbol: str, start: date, end: date) -> int:
        if type(start) == str:
            start = date_parser(start)  # type: ignore
        if type(end) == str:
            end = date_parser(end)  # type: ignore

        return (
            (end - start).days + 1
            if _is_crypto_symbol(symbol)
            else len(
                pd.date_range(
                    start,
                    end,
                    freq=pd.tseries.offsets.CustomBusinessDay(
                        holidays=self.get_trading_holidays()
                    ),
                )
            )
        )

    def get_max_data_points_per_load(self) -> int:
        return 10000

    def trading_days_slice(self, symbol: str, s: slice) -> slice:
        if _is_crypto_symbol(symbol):
            return s

        if s.start in self.datetime_cache and s.stop in self.datetime_cache:
            return slice(
                self.datetime_cache[s.start], self.datetime_cache[s.stop]
            )

        trading_days = self.trading_client.get_calendar(
            GetCalendarRequest(start=s.start.date(), end=s.stop.date())
        )
        if not trading_days:
            return s

        new_slice = slice(
            nytz.localize(
                datetime.combine(
                    trading_days[0].date, trading_days[0].open.time()
                )
            ),
            nytz.localize(
                datetime.combine(
                    trading_days[-1].date, trading_days[-1].open.time()
                )
            ),
        )

        self.datetime_cache[s.start] = new_slice.start
        self.datetime_cache[s.stop] = new_slice.stop

        return new_slice

    def crypto_get_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date,
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        tf = TimeFrame.Minute if scale == TimeScale.minute else TimeFrame.Day
        if "/" not in symbol:
            symbol = f"{symbol[:3]}/{symbol[3:]}"
        symbol = symbol.upper()
        _start, _end = self._localize_start_end(start, end)
        req = CryptoBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=date_parser(_start),
            end=date_parser(_end),
        )
        bars = self.crypto_historical_client.get_crypto_bars(req)
        data = bars.df
        if data.empty:
            raise ValueError(
                f"{symbol} has no crypto data for {start} to {end}"
            )
        if isinstance(data.index, pd.MultiIndex):
            data = (
                data.xs(symbol, level="symbol")
                if symbol in data.index.levels[0]
                else data.reset_index(level=0, drop=True)
            )
        data.index = pd.to_datetime(data.index).tz_convert("America/New_York")
        data["average"] = data.vwap
        data["count"] = data.trade_count
        data["vwap"] = np.nan
        return data[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "count",
                "average",
                "vwap",
            ]
        ]

    def get_symbols_data(
        self,
        symbols: List[str],
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> Dict[str, pd.DataFrame]:
        if not isinstance(symbols, list):
            raise AssertionError(f"{symbols} must be a list")

        if scale == TimeScale.minute:
            end += timedelta(days=1)
        _start, _end = self._localize_start_end(start, end)
        dfs: Dict = {}
        tf: TimeFrame = (
            TimeFrame.Minute
            if scale == TimeScale.minute
            else TimeFrame.Day
            if scale == TimeScale.day
            else None
        )
        try:
            req = StockBarsRequest(
                symbol_or_symbols=symbols,
                timeframe=tf,
                start=date_parser(_start),
                end=date_parser(_end),
                adjustment=Adjustment.ALL,
            )
            bars = self.stock_historical_client.get_stock_bars(req)
            data = bars.df
        except requests.exceptions.HTTPError as e:
            tlog(f"received HTTPError: {e}")
            if e.response.status_code in (500, 502, 504, 429):
                tlog("retrying")
                time.sleep(10)
                return self.get_symbols_data(symbols, start, end, scale)
            raise

        if data.empty:
            return dfs

        if isinstance(data.index, pd.MultiIndex):
            data = data.reset_index()
            data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_convert(
                "America/New_York"
            )
            data = data.set_index("timestamp")
            data["average"] = data.vwap
            data["count"] = data.trade_count
            data["vwap"] = np.nan
            grouped = data.groupby("symbol")
            for symbol in data["symbol"].unique():
                dfs[symbol] = grouped.get_group(symbol)[
                    [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "count",
                        "average",
                        "vwap",
                    ]
                ]
        else:
            data.index = pd.to_datetime(data.index).tz_convert(
                "America/New_York"
            )
            data["average"] = data.vwap
            data["count"] = data.trade_count
            data["vwap"] = np.nan
            if len(symbols) == 1:
                dfs[symbols[0]] = data[
                    [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "count",
                        "average",
                        "vwap",
                    ]
                ]

        return dfs

    def get_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        if _is_crypto_symbol(symbol):
            return self.crypto_get_symbol_data(
                symbol=symbol, start=start, end=end, scale=scale
            )

        _start, _end = self._localize_start_end(start, end)

        tf: TimeFrame = (
            TimeFrame.Minute
            if scale == TimeScale.minute
            else TimeFrame.Day
            if scale == TimeScale.day
            else None
        )

        try:
            if config.detailed_dl_debug_enabled:
                tlog(
                    f"symbol={symbol}, timeframe={tf}, range=({_start, _end})"
                )

            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=date_parser(_start),
                end=date_parser(_end),
                adjustment=Adjustment.ALL,
            )
            bars = self.stock_historical_client.get_stock_bars(req)
            data = bars.df
        except requests.exceptions.HTTPError as e:
            tlog(f"received HTTPError: {e}")
            if e.response.status_code in (500, 502, 504, 429):
                tlog("retrying")
                time.sleep(10)
                return self.get_symbol_data(symbol, start, end, scale)
            else:
                raise ValueError(
                    f"[EXCEPTION] {e} for {symbol} could not obtain data for {_start} to {_end} w {scale.name}"
                )
        except Exception as e:
            raise ValueError(
                f"[EXCEPTION] {e} for {symbol} has no data for {_start} to {_end} w {scale.name}"
            )
        else:
            if data.empty:
                raise ValueError(
                    f"[ERROR] {symbol} has no data for {_start} to {_end} w {scale.name}"
                )

        if isinstance(data.index, pd.MultiIndex):
            data = (
                data.xs(symbol, level="symbol")
                if symbol in data.index.levels[0]
                else data.reset_index(level=0, drop=True)
            )

        data.index = pd.to_datetime(data.index).tz_convert("America/New_York")
        data["average"] = data.vwap
        data["count"] = data.trade_count
        data["vwap"] = np.nan

        return data[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "count",
                "average",
                "vwap",
            ]
        ]


class AlpacaStream(StreamingAPI):
    def __init__(self, queues: QueueMapper):
        data_feed = (
            DataFeed.SIP
            if getattr(config, "alpaca_data_feed", "iex").lower() == "sip"
            else DataFeed.DELAYED_SIP
            if getattr(config, "alpaca_data_feed", "iex").lower()
            == "delayed_sip"
            else DataFeed.OTC
            if getattr(config, "alpaca_data_feed", "iex").lower() == "otc"
            else DataFeed.IEX
        )
        api_key = config.alpaca_api_key or "PKDUMMYKEY00000000000"
        secret_key = config.alpaca_api_secret or "SKDUMMYSECRET000000000000000000000000"
        self.stock_ws_client = StockDataStream(
            api_key=api_key,
            secret_key=secret_key,
            feed=data_feed,
        )
        self.crypto_ws_client = CryptoDataStream(
            api_key=api_key,
            secret_key=secret_key,
        )

        self.tasks: List[asyncio.Task] = []
        super().__init__(queues)

    async def run(self):
        if not self.tasks:
            if self.queues:
                self.tasks.append(
                    asyncio.create_task(self.stock_ws_client._run_forever())
                )
                self.tasks.append(
                    asyncio.create_task(self.crypto_ws_client._run_forever())
                )
            else:
                raise AssertionError(
                    "can't call `AlpacaStream.run()` without queues"
                )

    @classmethod
    async def bar_handler(cls, msg):
        try:
            event = {
                "symbol": msg.symbol,
                "open": msg.open,
                "close": msg.close,
                "high": msg.high,
                "low": msg.low,
                "timestamp": pd.to_datetime(
                    msg.timestamp, utc=True
                ).astimezone(nytz),
                "volume": msg.volume,
                "count": int(msg.trade_count or 0),
                "vwap": np.nan,
                "average": msg.vwap,
                "totalvolume": None,
                "EV": "AM",
            }
            cls.get_instance().queues[msg.symbol].put(event, timeout=1)
        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def crypto_bar_handler(cls, msg):
        try:
            if getattr(msg, "exchange", None) and msg.exchange != "CBSE":
                return

            event = {
                "symbol": msg.symbol,
                "open": msg.open,
                "close": msg.close,
                "high": msg.high,
                "low": msg.low,
                "timestamp": pd.to_datetime(
                    msg.timestamp, utc=True
                ).astimezone(nytz),
                "volume": msg.volume,
                "count": int(msg.trade_count or 0),
                "vwap": np.nan,
                "average": msg.vwap,
                "totalvolume": None,
                "EV": "AM",
            }
            cls.get_instance().queues[msg.symbol].put(event, timeout=1)
        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def trades_handler(cls, msg):
        try:
            ts = pd.to_datetime(msg.timestamp)
            if (time_diff := (datetime.now(tz=nytz) - ts)) > timedelta(
                seconds=10
            ) and randint(  # nosec
                1, 100
            ) == 1:  # nosec
                tlog(
                    f"Received trade for {msg.symbol} too out of sync w {time_diff}"
                )

            event = {
                "symbol": msg.symbol,
                "price": float(msg.price),
                "open": float(msg.price),
                "close": float(msg.price),
                "high": float(msg.price),
                "low": float(msg.price),
                "timestamp": ts,
                "volume": float(msg.size),
                "exchange": getattr(msg, "exchange", None),
                "conditions": getattr(msg, "conditions", None),
                "tape": getattr(msg, "tape", None),
                "average": np.nan,
                "count": 1,
                "vwap": np.nan,
                "EV": "T",
            }

            cls.get_instance().queues[msg.symbol].put(event, block=False)

        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def crypto_trades_handler(cls, msg):
        try:
            if getattr(msg, "exchange", None) and msg.exchange != "CBSE":
                return

            ts = pd.to_datetime(msg.timestamp)
            if (time_diff := (datetime.now(tz=nytz) - ts)) > timedelta(
                seconds=10
            ) and randint(  # nosec
                1, 100
            ) == 1:  # nosec
                tlog(
                    f"Received trade for {msg.symbol} too out of sync w {time_diff}"
                )

            event = {
                "symbol": msg.symbol,
                "price": float(msg.price),
                "open": float(msg.price),
                "close": float(msg.price),
                "high": float(msg.price),
                "low": float(msg.price),
                "timestamp": ts,
                "volume": float(msg.size),
                "exchange": getattr(msg, "exchange", None),
                "conditions": getattr(msg, "conditions", None),
                "tape": getattr(msg, "tape", None),
                "average": np.nan,
                "count": 1,
                "vwap": np.nan,
                "EV": "T",
            }

            cls.get_instance().queues[msg.symbol].put(event, block=False)

        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def quotes_handler(cls, msg):
        pass

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        tlog(f"Starting subscription for {len(symbols)} symbols")
        upper_symbols = [symbol.upper() for symbol in symbols]
        for syms in chunks(upper_symbols, 1000):
            tlog(f"\tsubscribe {len(syms)}/{len(upper_symbols)}")

            crypto_symbols = list(filter(_is_crypto_symbol, syms))
            equity_symbols = [x for x in syms if x not in crypto_symbols]

            for event in events:
                if event == WSEventType.MIN_AGG:
                    if crypto_symbols:
                        self.crypto_ws_client.subscribe_bars(
                            AlpacaStream.crypto_bar_handler,
                            *crypto_symbols,
                        )
                    if equity_symbols:
                        self.stock_ws_client.subscribe_bars(
                            AlpacaStream.bar_handler,
                            *equity_symbols,
                        )
                elif event == WSEventType.TRADE:
                    if crypto_symbols:
                        self.crypto_ws_client.subscribe_trades(
                            AlpacaStream.crypto_trades_handler, *crypto_symbols
                        )
                    if equity_symbols:
                        self.stock_ws_client.subscribe_trades(
                            AlpacaStream.trades_handler, *equity_symbols
                        )
                elif event == WSEventType.QUOTE:
                    if crypto_symbols:
                        self.crypto_ws_client.subscribe_quotes(
                            AlpacaStream.quotes_handler, *crypto_symbols
                        )
                    if equity_symbols:
                        self.stock_ws_client.subscribe_quotes(
                            AlpacaStream.quotes_handler, *equity_symbols
                        )

            await asyncio.sleep(1)

        tlog(f"Completed subscription for {len(symbols)} symbols")
        return True

    async def close(self) -> None:
        tlog("Closing AlpacaStream")
        try:
            await self.stock_ws_client.stop_ws()
        except Exception as e:
            tlog(f"Error stopping stock_ws: {e}")
        try:
            await self.crypto_ws_client.stop_ws()
        except Exception as e:
            tlog(f"Error stopping crypto_ws: {e}")

        for task in self.tasks:
            while not task.done():
                await asyncio.sleep(0.5)

        tlog("Tasks Done. Closed AlpacaStream")
