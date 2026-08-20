from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, Self

import httpx


@dataclass(frozen=True, slots=True)
class RetryConfig:
    max_retries: int = 3
    base_sec: float = 0.25
    max_sec: float = 10.0

    def backoff(self, attempt: int) -> float:
        base = min(self.base_sec * (2**attempt), self.max_sec)
        return max(0.0, base + random.uniform(-0.25 * base, 0.25 * base))


class MarketDataHTTPClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 15.0,
        retry_config: RetryConfig | None = None,
        user_agent: str = "pinelib-marketdata/0.1",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retry_config = retry_config or RetryConfig()
        self.user_agent = user_agent
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"User-Agent": self.user_agent},
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if self._client is None:
            raise RuntimeError("Use async with MarketDataHTTPClient")
        last: Exception | None = None
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                resp = await self._client.get(path, params=params)
                if resp.status_code == 429:
                    await asyncio.sleep(float(resp.headers.get("Retry-After", "1")))
                    continue
                if resp.status_code >= 500 and attempt < self.retry_config.max_retries:
                    await asyncio.sleep(self.retry_config.backoff(attempt))
                    continue
                resp.raise_for_status()
                return resp.json()
            except (
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.HTTPStatusError,
            ) as e:
                last = e
                if attempt < self.retry_config.max_retries:
                    await asyncio.sleep(self.retry_config.backoff(attempt))
        raise last or RuntimeError("HTTP request failed")
