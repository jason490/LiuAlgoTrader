import asyncio
import os
import queue
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (AssetClass, AssetStatus, OrderClass,
                                  OrderSide, OrderStatus, PositionSide,
                                  TimeInForce, TradeEvent)
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.models import TradeUpdate
from alpaca.trading.requests import (GetAssetsRequest, GetCalendarRequest,
                                     LimitOrderRequest, MarketOrderRequest,
                                     OrderRequest, StopLimitOrderRequest,
                                     StopLossRequest, StopOrderRequest,
                                     TakeProfitRequest,
                                     TrailingStopOrderRequest)
from alpaca.trading.stream import TradingStream
from pytz import timezone
from requests.auth import HTTPBasicAuth

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import Order, QueueMapper, Trade
from liualgotrader.trading.base import Trader

nyc = timezone("America/New_York")


class AlpacaTrader(Trader):
    def __init__(self, qm: QueueMapper = None):
        self.market_open: Optional[datetime]
        self.market_close: Optional[datetime]
        self.alpaca_brokage_api_baseurl = os.getenv(
            "ALPACA_BROKER_API_BASEURL", None
        )
        self.alpaca_brokage_api_key = os.getenv("ALPACA_BROKER_API_KEY", None)
        self.alpaca_brokage_api_secret = os.getenv(
            "ALPACA_BROKER_API_SECRET", None
        )

        paper = (
            "paper" in config.alpaca_base_url.lower()
            or "staging" in config.alpaca_base_url.lower()
        )
        api_key = config.alpaca_api_key or "PKDUMMYKEY00000000000"
        secret_key = config.alpaca_api_secret or "SKDUMMYSECRET000000000000000000000000"
        self.trading_client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
        )
        self.trading_stream: Optional[TradingStream] = None
        if qm:
            self.trading_stream = TradingStream(
                api_key=api_key,
                secret_key=secret_key,
                paper=paper,
            )
            if not self.trading_stream:
                raise AssertionError(
                    "Failed to authenticate Alpaca web_socket client"
                )
            self.trading_stream.subscribe_trade_updates(
                AlpacaTrader.trade_update_handler
            )
        self.running_task: Optional[asyncio.Task] = None

        now = datetime.now(nyc)
        try:
            calendars = self.trading_client.get_calendar(
                GetCalendarRequest(start=now.date(), end=now.date())
            )
        except Exception:
            calendars = None

        if calendars and len(calendars) > 0:
            calendar = calendars[0]
            if now.date() >= calendar.date:
                self.market_open = now.replace(
                    hour=calendar.open.hour,
                    minute=calendar.open.minute,
                    second=0,
                    microsecond=0,
                )
                self.market_close = now.replace(
                    hour=calendar.close.hour,
                    minute=calendar.close.minute,
                    second=0,
                    microsecond=0,
                )
            else:
                self.market_open = self.market_close = None
        else:
            self.market_open = self.market_close = None

        super().__init__(qm)

    async def _is_personal_order_completed(
        self, order_id: str
    ) -> Tuple[Order.EventType, float, float, float]:
        alpaca_order = self.trading_client.get_order_by_id(order_id=order_id)
        status_str = (
            str(alpaca_order.status).lower().replace("orderstatus.", "")
        )
        event = (
            Order.EventType.canceled
            if status_str in ["canceled", "expired", "replaced"]
            else Order.EventType.pending
            if status_str
            in [
                "pending_cancel",
                "pending_replace",
                "pending_new",
                "accepted",
                "accepted_for_bidding",
                "held",
            ]
            else Order.EventType.fill
            if status_str == "filled"
            else Order.EventType.partial_fill
            if status_str == "partially_filled"
            else Order.EventType.other
        )
        return (
            event,
            float(alpaca_order.filled_avg_price or 0.0),
            float(alpaca_order.filled_qty or 0.0),
            0.0,
        )

    async def is_fractionable(self, symbol: str) -> bool:
        try:
            asset_details = self.trading_client.get_asset(symbol.upper())
            return bool(asset_details.fractionable)
        except Exception:
            return False

    async def _is_brokerage_account_order_completed(
        self, order_id: str, external_order_id: Optional[str] = None
    ) -> Tuple[Order.EventType, float, float, float]:
        if not self.alpaca_brokage_api_baseurl:
            raise AssertionError(
                "order_on_behalf can't be called, if brokerage configs incomplete"
            )

        endpoint: str = (
            f"/v1/trading/accounts/{external_order_id}/orders/{order_id}"
        )
        tlog(f"_is_brokerage_account_order_completed:{endpoint}")
        url: str = self.alpaca_brokage_api_baseurl + endpoint

        response = await self._get_request(url)
        tlog(f"_is_brokerage_account_order_completed: response: {response}")
        event = (
            Order.EventType.canceled
            if response["status"] in ["canceled", "expired", "replaced"]
            else Order.EventType.pending
            if response["status"] in ["pending_cancel", "pending_replace"]
            else Order.EventType.fill
            if response["status"] == "filled"
            else Order.EventType.partial_fill
            if response["status"] == "partially_filled"
            else Order.EventType.other
        )
        return (
            event,
            float(response.get("filled_avg_price") or 0.0),
            float(response.get("filled_qty") or 0.0),
            0.0,
        )

    async def is_order_completed(
        self, order_id: str, external_order_id: Optional[str] = None
    ) -> Tuple[Order.EventType, float, float, float]:
        return (
            await self._is_brokerage_account_order_completed(
                order_id, external_order_id
            )
            if external_order_id
            else await self._is_personal_order_completed(order_id)
        )

    def get_market_schedule(
        self,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        return self.market_open, self.market_close

    def get_trading_days(
        self, start_date: date, end_date: date = date.today()
    ) -> pd.DataFrame:
        calendars = self.trading_client.get_calendar(
            GetCalendarRequest(start=start_date, end=end_date)
        )
        records = []
        for cal in calendars:
            records.append(
                {
                    "date": cal.date,
                    "open": cal.open.strftime("%H:%M"),
                    "close": cal.close.strftime("%H:%M"),
                    "session_open": cal.open.strftime("%H:%M"),
                    "session_close": cal.close.strftime("%H:%M"),
                }
            )
        _df = pd.DataFrame.from_records(records)
        if not _df.empty:
            _df["date"] = pd.to_datetime(_df.date)
            return _df.set_index("date")
        return pd.DataFrame()

    def get_position(self, symbol: str) -> float:
        pos = self.trading_client.get_open_position(symbol.upper())
        side_str = str(pos.side).lower().replace("positionside.", "")
        return float(pos.qty) if side_str == "long" else -1.0 * float(pos.qty)

    def to_order(self, alpaca_order: AlpacaOrder) -> Order:
        status_str = (
            str(alpaca_order.status).lower().replace("orderstatus.", "")
        )
        event = (
            Order.EventType.canceled
            if status_str in ["canceled", "expired", "replaced"]
            else Order.EventType.pending
            if status_str
            in [
                "pending_cancel",
                "pending_replace",
                "pending_new",
                "accepted",
                "accepted_for_bidding",
                "held",
            ]
            else Order.EventType.fill
            if status_str == "filled"
            else Order.EventType.partial_fill
            if status_str == "partially_filled"
            else Order.EventType.other
        )
        side_str = str(alpaca_order.side).lower().replace("orderside.", "")
        return Order(
            order_id=str(alpaca_order.id),
            symbol=alpaca_order.symbol.lower(),
            event=event,
            price=float(alpaca_order.limit_price or 0.0),
            side=Order.FillSide[side_str],
            filled_qty=float(alpaca_order.filled_qty or 0.0),
            remaining_amount=float(alpaca_order.qty or 0.0)
            - float(alpaca_order.filled_qty or 0.0),
            submitted_at=alpaca_order.submitted_at,
            avg_execution_price=float(alpaca_order.filled_avg_price)
            if alpaca_order.filled_avg_price is not None
            else None,
            trade_fees=0.0,
        )

    def _json_to_order(
        self,
        brokerage_response: dict,
        external_account_id: Optional[str] = None,
    ) -> Order:
        event = (
            Order.EventType.canceled
            if brokerage_response["status"]
            in ["canceled", "expired", "replaced"]
            else Order.EventType.pending
            if brokerage_response["status"]
            in ["pending_cancel", "pending_replace"]
            else Order.EventType.fill
            if brokerage_response["status"] == "filled"
            else Order.EventType.partial_fill
            if brokerage_response["status"] == "partially_filled"
            else Order.EventType.other
        )
        return Order(
            order_id=brokerage_response["id"],
            symbol=brokerage_response["symbol"].lower(),
            event=event,
            price=float(brokerage_response["limit_price"] or 0.0),
            side=Order.FillSide[brokerage_response["side"]],
            filled_qty=float(brokerage_response["filled_qty"]),
            remaining_amount=float(brokerage_response["qty"])
            - float(brokerage_response["filled_qty"]),
            submitted_at=pd.Timestamp(
                ts_input=brokerage_response["submitted_at"],
                unit="ms",
                tz="US/Eastern",
            ),
            avg_execution_price=brokerage_response["filled_avg_price"],
            trade_fees=0.0,
            external_account_id=external_account_id,
        )

    async def get_order(self, order_id: str) -> Order:
        return self.to_order(self.trading_client.get_order_by_id(order_id))

    def is_market_open_today(self) -> bool:
        return self.market_open is not None

    def get_time_market_close(self) -> Optional[timedelta]:
        if not self.is_market_open_today():
            raise AssertionError("Market closed today")

        return (
            self.market_close - datetime.now(nyc)
            if self.market_close
            else None
        )

    async def reconnect(self):
        paper = (
            "paper" in config.alpaca_base_url.lower()
            or "staging" in config.alpaca_base_url.lower()
        )
        self.trading_client = TradingClient(
            api_key=config.alpaca_api_key,
            secret_key=config.alpaca_api_secret,
            paper=paper,
        )

    async def run(self) -> asyncio.Task:
        if not self.running_task:
            tlog("starting Alpaca listener")
            if not self.trading_stream:
                raise AssertionError("Must initialize with QueueMapper")
            self.running_task = asyncio.create_task(
                self.trading_stream._run_forever()
            )
        return self.running_task

    async def close(self):
        if not self.trading_stream:
            return
        if self.running_task:
            await self.trading_stream.stop_ws()

    async def get_tradeable_symbols(self) -> List[str]:
        data = self.trading_client.get_all_assets()
        return [asset.symbol.lower() for asset in data if asset.tradable]

    async def get_shortable_symbols(self) -> List[str]:
        data = self.trading_client.get_all_assets()
        return [
            asset.symbol.lower()
            for asset in data
            if asset.tradable and asset.easy_to_borrow and asset.shortable
        ]

    async def is_shortable(self, symbol) -> bool:
        asset = self.trading_client.get_asset(symbol.upper())
        return (
            asset.tradable is not False
            and asset.shortable is not False
            and asset.status != AssetStatus.INACTIVE
            and asset.status != "inactive"
            and asset.easy_to_borrow is not False
        )

    async def _cancel_personal_order(self, order_id: str) -> bool:
        self.trading_client.cancel_order_by_id(order_id)
        return True

    async def _cancel_brokerage_order(
        self, account_id: str, order_id: str
    ) -> bool:
        if not self.alpaca_brokage_api_baseurl:
            raise AssertionError(
                "_cancel_brokerage_order can't be called, if brokerage configs incomplete"
            )

        endpoint: str = f"/v1/trading/accounts/{account_id}/orders/{order_id}"
        url: str = self.alpaca_brokage_api_baseurl + endpoint

        response_code = await self._delete_request(url)
        tlog(
            f"cancel_brokerage_order {account_id},{order_id} -> {response_code}"
        )
        return response_code == 204

    async def cancel_order(self, order: Order) -> bool:
        if order.external_account_id:
            return await self._cancel_brokerage_order(
                order.external_account_id, order.order_id
            )

        return await self._cancel_personal_order(order.order_id)

    async def _personal_submit(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str,
        time_in_force: str,
        limit_price: str = None,
        stop_price: str = None,
        client_order_id: str = None,
        extended_hours: bool = None,
        order_class: str = None,
        take_profit: dict = None,
        stop_loss: dict = None,
        trail_price: str = None,
        trail_percent: str = None,
        on_behalf_of: str = None,
    ) -> Order:
        order_side = (
            OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
        )
        tif = (
            TimeInForce(time_in_force.lower())
            if time_in_force
            else TimeInForce.DAY
        )
        oc = OrderClass(order_class.lower()) if order_class else None
        tp = (
            TakeProfitRequest(**take_profit)
            if isinstance(take_profit, dict)
            else None
        )
        sl = (
            StopLossRequest(**stop_loss)
            if isinstance(stop_loss, dict)
            else None
        )

        req: OrderRequest
        if order_type.lower() == "market":
            req = MarketOrderRequest(
                symbol=symbol.upper(),
                qty=float(qty),
                side=order_side,
                time_in_force=tif,
                extended_hours=extended_hours,
                client_order_id=client_order_id,
                order_class=oc,
                take_profit=tp,
                stop_loss=sl,
            )
        elif order_type.lower() == "limit":
            req = LimitOrderRequest(
                symbol=symbol.upper(),
                qty=float(qty),
                side=order_side,
                time_in_force=tif,
                limit_price=float(limit_price)
                if limit_price is not None
                else None,
                extended_hours=extended_hours,
                client_order_id=client_order_id,
                order_class=oc,
                take_profit=tp,
                stop_loss=sl,
            )
        elif order_type.lower() == "stop":
            req = StopOrderRequest(
                symbol=symbol.upper(),
                qty=float(qty),
                side=order_side,
                time_in_force=tif,
                stop_price=float(stop_price) if stop_price is not None else None,
                extended_hours=extended_hours,
                client_order_id=client_order_id,
                order_class=oc,
                take_profit=tp,
                stop_loss=sl,
            )
        elif order_type.lower() == "stop_limit":
            req = StopLimitOrderRequest(
                symbol=symbol.upper(),
                qty=float(qty),
                side=order_side,
                time_in_force=tif,
                stop_price=float(stop_price) if stop_price is not None else None,
                limit_price=float(limit_price)
                if limit_price is not None
                else None,
                extended_hours=extended_hours,
                client_order_id=client_order_id,
                order_class=oc,
                take_profit=tp,
                stop_loss=sl,
            )
        elif order_type.lower() == "trailing_stop":
            req = TrailingStopOrderRequest(
                symbol=symbol.upper(),
                qty=float(qty),
                side=order_side,
                time_in_force=tif,
                trail_price=float(trail_price)
                if trail_price is not None
                else None,
                trail_percent=float(trail_percent)
                if trail_percent is not None
                else None,
                extended_hours=extended_hours,
                client_order_id=client_order_id,
                order_class=oc,
                take_profit=tp,
                stop_loss=sl,
            )
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

        o = self.trading_client.submit_order(req)
        return self.to_order(o)

    async def _post_request(self, url: str, payload: Dict) -> Dict:
        response = requests.post(
            url=url,
            json=payload,
            auth=HTTPBasicAuth(
                self.alpaca_brokage_api_key, self.alpaca_brokage_api_secret
            ),
        )

        if response.status_code in (429, 504):
            if "x-ratelimit-reset" in response.headers:
                tlog(
                    f"ALPACA BROKERAGE rate-limit till {response.headers['x-ratelimit-reset']}"
                )
                await asyncio.sleep(
                    int(time.time())
                    - int(response.headers["x-ratelimit-reset"])
                )
                tlog("ALPACA BROKERAGE going to retry")
            else:
                tlog(
                    f"ALPACA BROKERAGE push-back w/ {response.status_code} and no x-ratelimit-reset header"
                )
                await asyncio.sleep(10.0)

            return await self._post_request(url, payload)

        if response.status_code in (200, 201, 204):
            return response.json()

        raise AssertionError(
            f"HTTP ERROR {response.status_code} from ALPACA BROKERAGE API with error {response.text}"
        )

    async def _get_request(self, url: str) -> Dict:
        response = requests.get(
            url=url,
            auth=HTTPBasicAuth(
                self.alpaca_brokage_api_key, self.alpaca_brokage_api_secret
            ),
        )

        if response.status_code in (429, 504):
            if "x-ratelimit-reset" in response.headers:
                tlog(
                    f"ALPACA BROKERAGE rate-limit till {response.headers['x-ratelimit-reset']}"
                )
                await asyncio.sleep(
                    int(time.time())
                    - int(response.headers["x-ratelimit-reset"])
                )
                tlog("ALPACA BROKERAGE going to retry")
            else:
                tlog(
                    f"ALPACA BROKERAGE push-back w/ {response.status_code} and no x-ratelimit-reset header"
                )
                await asyncio.sleep(10.0)

            return await self._get_request(url)

        if response.status_code in (200, 201, 204):
            return response.json()

        raise AssertionError(
            f"HTTP ERROR {response.status_code} from ALPACA BROKERAGE API with error {response.text}"
        )

    async def _delete_request(self, url: str) -> int:
        response = requests.delete(
            url=url,
            auth=HTTPBasicAuth(
                self.alpaca_brokage_api_key, self.alpaca_brokage_api_secret
            ),
        )
        # TODO: create a decorator the the re-try / push-backs from server instead of copying.
        if response.status_code in (429, 504):
            if "x-ratelimit-reset" in response.headers:
                tlog(
                    f"ALPACA BROKERAGE rate-limit till {response.headers['x-ratelimit-reset']}"
                )
                await asyncio.sleep(
                    int(time.time())
                    - int(response.headers["x-ratelimit-reset"])
                )
                tlog("ALPACA BROKERAGE going to retry")
            else:
                tlog(
                    f"ALPACA BROKERAGE push-back w/ {response.status_code} and no x-ratelimit-reset header"
                )
                await asyncio.sleep(10.0)

            return await self._delete_request(url)

        return response.status_code

    async def _order_on_behalf(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str,
        time_in_force: str,
        limit_price: str = None,
        stop_price: str = None,
        client_order_id: str = None,
        extended_hours: bool = None,
        order_class: str = None,
        take_profit: dict = None,
        stop_loss: dict = None,
        trail_price: str = None,
        trail_percent: str = None,
        on_behalf_of: str = None,
    ) -> Order:
        if not self.alpaca_brokage_api_baseurl:
            raise AssertionError(
                "order_on_behalf can't be called, if brokerage configs incomplete"
            )

        endpoint: str = f"/v1/trading/accounts/{on_behalf_of}/orders"
        url: str = self.alpaca_brokage_api_baseurl + endpoint

        payload = {
            "symbol": symbol.upper(),
            "qty": qty,
            "side": side,
            "type": order_type,
        }

        if limit_price:
            payload["limit_price"] = limit_price
        if time_in_force:
            payload["time_in_force"] = time_in_force

        json_response: Dict = await self._post_request(
            url=url, payload=payload
        )
        tlog(f"ALPACA BROKERAGE RESPONSE: {json_response}")

        return self._json_to_order(json_response, on_behalf_of)

    async def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str,
        time_in_force: str = "day",
        limit_price: str = None,
        stop_price: str = None,
        client_order_id: str = None,
        extended_hours: bool = None,
        order_class: str = None,
        take_profit: dict = None,
        stop_loss: dict = None,
        trail_price: str = None,
        trail_percent: str = None,
        on_behalf_of: str = None,
    ) -> Order:
        if on_behalf_of:
            return await self._order_on_behalf(
                symbol,
                qty,
                side,
                order_type,
                time_in_force,
                limit_price,
                stop_price,
                client_order_id,
                extended_hours,
                order_class,
                take_profit,
                stop_loss,
                trail_price,
                trail_percent,
                on_behalf_of,
            )
        else:
            return await self._personal_submit(
                symbol,
                qty,
                side,
                order_type,
                time_in_force,
                limit_price,
                stop_price,
                client_order_id,
                extended_hours,
                order_class,
                take_profit,
                stop_loss,
                trail_price,
                trail_percent,
                on_behalf_of,
            )

    @classmethod
    def _trade_from_dict(cls, trade_update: TradeUpdate) -> Optional[Trade]:
        event_str = (
            str(trade_update.event).lower().replace("tradeevent.", "")
        )
        if event_str == "new":
            return None

        alpaca_order = trade_update.order
        if isinstance(alpaca_order, dict):
            symbol = alpaca_order["symbol"].lower().replace("/", "")
            order_id = str(alpaca_order["id"])
            filled_avg_price = float(
                alpaca_order.get("filled_avg_price") or 0.0
            )
            updated_at = pd.Timestamp(
                ts_input=alpaca_order.get("updated_at")
                or trade_update.timestamp,
                tz="US/Eastern",
            )
            side_str = (
                str(alpaca_order["side"]).lower().replace("orderside.", "")
            )
        else:
            symbol = alpaca_order.symbol.lower().replace("/", "")
            order_id = str(alpaca_order.id)
            filled_avg_price = float(alpaca_order.filled_avg_price or 0.0)
            updated_at = pd.Timestamp(
                ts_input=alpaca_order.updated_at or trade_update.timestamp,
                tz="US/Eastern",
            )
            side_str = (
                str(alpaca_order.side).lower().replace("orderside.", "")
            )

        event = (
            Order.EventType.canceled
            if event_str
            in ["canceled", "suspended", "expired", "cancel_rejected"]
            else Order.EventType.rejected
            if event_str == "rejected"
            else Order.EventType.fill
            if event_str == "fill"
            else Order.EventType.partial_fill
            if event_str == "partial_fill"
            else Order.EventType.other
        )

        return Trade(
            order_id=order_id,
            symbol=symbol,
            event=event,
            filled_qty=float(trade_update.qty or 0.0)
            if event_str in ["fill", "partial_fill"]
            else 0.0,
            trade_fee=0.0,
            filled_avg_price=filled_avg_price,
            liquidity="",
            updated_at=updated_at,
            side=Order.FillSide[side_str],
        )

    @classmethod
    async def trade_update_handler(cls, data):
        try:
            trade = cls._trade_from_dict(data)
            if not trade:
                return

            to_send = {
                "EV": "trade_update",
                "symbol": trade.symbol,
                "trade": trade.__dict__,
            }
            for q in cls.get_instance().queues.get_allqueues():
                q.put(to_send, timeout=1)

        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {trade.symbol} is FULL:{f}, sleeping for 2 seconds and re-trying."
            )
            raise
        except Exception as e:
            tlog(f"[EXCEPTION] process_message(): exception {e}")
            if config.debug_enabled:
                traceback.print_exc()
