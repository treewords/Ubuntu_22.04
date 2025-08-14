import asyncio
import websockets
import json
import gzip
import io
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# ───────── Logging ─────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("bingx_ws")

# ───────── Config ─────────
WS_URL = "wss://open-api-swap.bingx.com/swap-market"
SUBSCRIBE_MSG = {
    "id": "client-uuid-123",
    "reqType": "sub",
    "dataType": "BTC-USDT@kline_3m"
}

# ───────── Structură Candelă ─────────
@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: int

    def readable_time(self) -> str:
        return datetime.utcfromtimestamp(self.timestamp // 1000).strftime('%Y-%m-%d %H:%M:%S')

# ───────── Producer WebSocket ─────────
class BingXWebSocketProducer:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue

    async def connect(self):
        while True:
            try:
                async with websockets.connect(WS_URL, max_size=None) as ws:
                    log.info("WebSocket connected")
                    await ws.send(json.dumps(SUBSCRIBE_MSG))
                    log.info("Subscribed to: %s", SUBSCRIBE_MSG["dataType"])
                    await self.listen(ws)
            except Exception as e:
                log.warning("WebSocket error: %s", e)
                await asyncio.sleep(5)

    def decompress(self, message: bytes) -> str:
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(message)) as f:
                return f.read().decode("utf-8")
        except Exception as e:
            log.error("Decompression error: %s", e)
            return ""

    async def listen(self, ws):
        async for message in ws:
            raw = self.decompress(message)
            await self.handle_message(raw, ws)

    async def handle_message(self, raw: str, ws):
        if not raw:
            return

        if "ping" in raw:
            try:
                obj = json.loads(raw)
                if "ping" in obj:
                    await ws.send(json.dumps({"pong": obj["ping"]}))
                    log.debug("Pong sent: %s", obj["ping"])
                return
            except Exception as e:
                log.warning("Ping error: %s", e)
                return

        try:
            obj = json.loads(raw)
            if "data" not in obj or "dataType" not in obj or "kline" not in obj["dataType"]:
                return

            data = obj["data"]
            if isinstance(data, list):
                for item in data:
                    candle = self.parse_candle(item)
                    if candle:
                        await self.queue.put(candle)
            else:
                candle = self.parse_candle(data)
                if candle:
                    await self.queue.put(candle)

        except Exception as e:
            log.error("Failed to parse message: %s", e)

    def parse_candle(self, data: dict) -> Optional[Candle]:
        try:
            k = data["kline"] if "kline" in data else data
            timestamp = int(k.get("t") or k.get("T"))  # acceptă și "T"

            return Candle(
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                timestamp=timestamp
            )
        except Exception as e:
            log.error("Invalid candle format: %s", e)
            return None

# ───────── Consumer care loghează doar când candelele sunt închise ─────────
class CandleConsumer:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.last_candle: Optional[Candle] = None

    async def run(self):
        while True:
            candle: Candle = await self.queue.get()
            await self.process(candle)
            self.queue.task_done()

    async def process(self, candle: Candle):
        if self.last_candle and candle.timestamp != self.last_candle.timestamp:
            # Logăm ultima candelă completă
            log.info(
                "[%s] O:%.2f H:%.2f L:%.2f C:%.2f V:%.2f",
                self.last_candle.readable_time(),
                self.last_candle.open, self.last_candle.high,
                self.last_candle.low, self.last_candle.close,
                self.last_candle.volume
            )
        self.last_candle = candle

# ───────── Main ─────────
async def main():
    queue = asyncio.Queue()
    producer = BingXWebSocketProducer(queue)
    consumer = CandleConsumer(queue)

    await asyncio.gather(
        producer.connect(),
        consumer.run()
    )

if __name__ == "__main__":
    asyncio.run(main())
