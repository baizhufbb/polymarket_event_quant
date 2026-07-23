from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import requests

from .models import Market


GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
BTC_FIVE_MINUTE_SERIES_ID = 10684
GAMMA_PAGE_SIZE = 100


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


class MarketDiscovery:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "polymarket-btc-bot/0.1"

    def discover(
        self,
        window_minutes: int = 40,
        *,
        farthest_first: bool = False,
        timeout: float = 20,
    ) -> list[Market]:
        now = datetime.now(timezone.utc)
        requested = math.ceil(window_minutes / 5) if farthest_first else 100
        params = {
            "series_id": BTC_FIVE_MINUTE_SERIES_ID,
            "active": "true",
            "closed": "false",
            "order": "endDate",
            "ascending": "false" if farthest_first else "true",
            "end_date_min": _iso(now - timedelta(minutes=5)),
        }
        if not farthest_first:
            params["end_date_max"] = _iso(
                now + timedelta(minutes=window_minutes)
            )

        events = []
        while len(events) < requested:
            page_size = min(GAMMA_PAGE_SIZE, requested - len(events))
            response = self.session.get(
                GAMMA_EVENTS,
                params=params | {"limit": page_size, "offset": len(events)},
                timeout=timeout,
            )
            response.raise_for_status()
            page = response.json()
            events.extend(page)
            if len(page) < page_size:
                break

        markets = [self._parse(event) for event in events]
        return sorted(
            (market for market in markets if market),
            key=lambda item: item.start_ts,
            reverse=farthest_first,
        )

    @staticmethod
    def _parse(event: dict) -> Market | None:
        market_rows = event.get("markets") or []
        if len(market_rows) != 1:
            return None
        market = market_rows[0]
        slug = str(event.get("slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            return None
        if not market.get("acceptingOrders"):
            return None
        if not market.get("enableOrderBook"):
            return None
        try:
            start_ts = int(slug.rsplit("-", 1)[1])
            token_ids = json.loads(market["clobTokenIds"])
            outcomes = json.loads(market["outcomes"])
            token_by_outcome = dict(zip(outcomes, token_ids, strict=True))
            return Market(
                slug=slug,
                condition_id=str(market["conditionId"]),
                start_ts=start_ts,
                end_ts=start_ts + 300,
                up_token_id=str(token_by_outcome["Up"]),
                down_token_id=str(token_by_outcome["Down"]),
                min_size=Decimal(str(market.get("orderMinSize") or 5)),
                tick_size=Decimal(str(market.get("orderPriceMinTickSize") or "0.01")),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None


def is_eligible(market: Market, *, run_started_ts: int, now_ts: int) -> bool:
    return market.start_ts >= run_started_ts and market.end_ts > now_ts
