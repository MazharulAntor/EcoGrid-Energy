"""
EcoGrid Energy – P2P Renewable Energy Trading Platform
ICT711 Advanced Software Engineering – Assessment 2
================================================
Demonstrates the Event-Driven Microservices architecture described in the
System Design Report. Each bounded context (Marketplace, IoT Ingestion,
Financial Settlement) is implemented as a separate class/module, communicating
exclusively through an in-process EventBus that simulates Kafka topics.

Bounded Contexts implemented:
  1. IoTIngestionService   – ingests meter readings, publishes MeterReadingReceived
  2. MarketplaceService    – matches sellers/buyers, publishes TradeMatched
  3. SettlementService     – processes trades, publishes SettlementConfirmed
  4. FitnessFunctionRunner – verifies architectural fitness functions

"""

from __future__ import annotations
import uuid
import time
import random
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Any
from collections import defaultdict

# ──────────────────────────────────────────────────────────────────────────────
# SHARED INFRASTRUCTURE
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
    datefmt="%H:%M:%S"
)


class EventBus:
    """
    In-process simulation of an Apache Kafka event bus.
    Supports topic-based publish/subscribe with at-least-once delivery semantics.
    In production, replace with a real Kafka client (confluent-kafka-python).
    """
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_log: List[Dict] = []          # Simulates Kafka's commit log
        self._processed: Dict[str, set] = defaultdict(set)  # DLQ deduplication
        self._lock = threading.Lock()

    def subscribe(self, topic: str, handler: Callable, consumer_group: str = "default"):
        with self._lock:
            self._subscribers[topic].append((handler, consumer_group))

    def publish(self, topic: str, event: Dict[str, Any]):
        """Publish an event to a topic. All subscribers receive a copy."""
        event.setdefault("eventId", str(uuid.uuid4()))
        event.setdefault("publishedAt", datetime.now(timezone.utc).isoformat())
        with self._lock:
            self._event_log.append({"topic": topic, "event": event})
            handlers = list(self._subscribers[topic])
        for handler, group in handlers:
            event_id = event["eventId"]
            dedupe_key = f"{group}:{event_id}"
            if dedupe_key not in self._processed[topic]:
                self._processed[topic].add(dedupe_key)
                try:
                    handler(event)
                except Exception as exc:
                    # Route to Dead Letter Queue
                    logging.getLogger("EventBus").error(
                        "DLQ: handler %s failed for event %s: %s",
                        handler.__name__, event_id, exc
                    )

    def replay(self, topic: str, from_offset: int = 0) -> List[Dict]:
        """Simulate Kafka offset replay."""
        return [e["event"] for e in self._event_log if e["topic"] == topic][from_offset:]


# Shared event bus instance (simulates Kafka cluster)
event_bus = EventBus()

# Kafka topic names – match the Context Map in the report
TOPIC_METER_READINGS   = "meter.readings"
TOPIC_MARKETPLACE      = "marketplace.events"
TOPIC_SETTLEMENT       = "settlement.events"
TOPIC_DEVICE_STATUS    = "meter.device.status"


# ──────────────────────────────────────────────────────────────────────────────
# DOMAIN MODELS
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class MeterReading:
    """Value Object – IoT Ingestion Bounded Context."""
    device_id: str
    household_id: str
    kwh_generated: float         # kWh produced by solar panels
    kwh_consumed: float          # kWh consumed by household
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def surplus_kwh(self) -> float:
        """Net surplus available for trading."""
        return max(0.0, round(self.kwh_generated - self.kwh_consumed, 4))

    @property
    def deficit_kwh(self) -> float:
        """Net energy shortfall – household needs to buy."""
        return max(0.0, round(self.kwh_consumed - self.kwh_generated, 4))


@dataclass
class TradeOffer:
    """Aggregate Root – Marketplace Bounded Context."""
    offer_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    household_id: str = ""
    offer_type: str = ""              # "SELL" | "BUY"
    kwh_amount: float = 0.0
    price_per_kwh: float = 0.0        # AUD cents
    grid_zone: str = ""
    status: str = "OPEN"              # OPEN | MATCHED | EXPIRED | CANCELLED
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class TradeMatch:
    """Entity – Marketplace Bounded Context."""
    match_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sell_offer_id: str = ""
    buy_offer_id: str = ""
    seller_id: str = ""
    buyer_id: str = ""
    kwh_traded: float = 0.0
    agreed_price_per_kwh: float = 0.0
    status: str = "PENDING_SETTLEMENT"
    matched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class SettlementRecord:
    """Aggregate Root – Financial Settlement Bounded Context."""
    settlement_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    match_id: str = ""
    seller_id: str = ""
    buyer_id: str = ""
    kwh_traded: float = 0.0
    price_per_kwh: float = 0.0
    total_amount_aud: float = 0.0
    status: str = "PENDING"           # PENDING | CONFIRMED | FAILED
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class WalletAccount:
    """Entity – Financial Settlement Bounded Context."""
    household_id: str
    balance_aud: float = 100.0        # Starting balance in AUD


# ──────────────────────────────────────────────────────────────────────────────
# BOUNDED CONTEXT 1: IoT INGESTION SERVICE
# ──────────────────────────────────────────────────────────────────────────────

class IoTIngestionService:
    """
    Responsible for real-time ingestion of smart meter data.
    Publishes validated readings to the 'meter.readings' Kafka topic.
    Monitors device health and publishes device status events.

    Architecture note: This service has NO knowledge of Marketplace or Settlement.
    It only knows about devices and readings.
    """

    def __init__(self, bus: EventBus):
        self._bus = bus
        self._log = logging.getLogger("IoTIngestionService")
        self._device_registry: Dict[str, datetime] = {}  # deviceId → last_seen
        self._ingestion_count = 0
        self._start_time = time.time()

    def register_device(self, device_id: str, household_id: str):
        """Register a smart meter device."""
        self._device_registry[device_id] = datetime.now(timezone.utc)
        self._log.info("Device registered: %s → household %s", device_id, household_id)

    def ingest_reading(self, device_id: str, household_id: str,
                       kwh_generated: float, kwh_consumed: float):
        """
        Validate and ingest a meter reading.
        Publishes MeterReadingReceived event to event bus.
        Fitness Function: p99 publish latency must be < 100ms.
        """
        t0 = time.perf_counter()

        # Validation (Anti-Corruption Layer)
        if kwh_generated < 0 or kwh_consumed < 0:
            self._log.warning("Invalid reading rejected from device %s", device_id)
            return
        if device_id not in self._device_registry:
            self._log.warning("Unknown device %s – registering automatically", device_id)
            self.register_device(device_id, household_id)

        reading = MeterReading(
            device_id=device_id,
            household_id=household_id,
            kwh_generated=round(kwh_generated, 4),
            kwh_consumed=round(kwh_consumed, 4)
        )

        # Update heartbeat
        self._device_registry[device_id] = datetime.now(timezone.utc)

        # Publish domain event to Kafka topic
        event = {
            "eventType": "MeterReadingReceived",
            "deviceId": device_id,
            "householdId": household_id,
            "kwhGenerated": reading.kwh_generated,
            "kwhConsumed": reading.kwh_consumed,
            "surplusKwh": reading.surplus_kwh,
            "deficitKwh": reading.deficit_kwh,
            "timestamp": reading.timestamp,
        }
        self._bus.publish(TOPIC_METER_READINGS, event)
        self._ingestion_count += 1

        latency_ms = (time.perf_counter() - t0) * 1000
        self._log.info(
            "Reading ingested | device=%s surplus=%.3fkWh deficit=%.3fkWh latency=%.2fms",
            device_id, reading.surplus_kwh, reading.deficit_kwh, latency_ms
        )

    def simulate_burst(self, n_devices: int = 10, readings_per_device: int = 5):
        """Simulate high-frequency IoT burst to validate scalability."""
        self._log.info("Simulating burst: %d devices × %d readings", n_devices, readings_per_device)
        t0 = time.perf_counter()
        for i in range(n_devices):
            did = f"METER-{i:04d}"
            hid = f"HH-{i:04d}"
            self.register_device(did, hid)
            for _ in range(readings_per_device):
                self.ingest_reading(
                    did, hid,
                    kwh_generated=round(random.uniform(0.1, 5.0), 3),
                    kwh_consumed=round(random.uniform(0.05, 4.5), 3)
                )
        elapsed = time.perf_counter() - t0
        rate = (n_devices * readings_per_device) / elapsed
        self._log.info(
            "Burst complete: %d readings in %.3fs = %.1f readings/sec",
            n_devices * readings_per_device, elapsed, rate
        )
        return rate


# ──────────────────────────────────────────────────────────────────────────────
# BOUNDED CONTEXT 2: MARKETPLACE SERVICE
# ──────────────────────────────────────────────────────────────────────────────

class MarketplaceService:
    """
    Manages the P2P energy trading marketplace.
    Matches SELL offers from solar households with BUY offers from consuming households.
    Publishes TradeMatched events when a match is found.

    Architecture note: This service subscribes to meter.readings events.
    It does NOT call the IoT service directly (no synchronous coupling).
    """

    def __init__(self, bus: EventBus):
        self._bus = bus
        self._log = logging.getLogger("MarketplaceService")
        self._offers: Dict[str, TradeOffer] = {}
        self._matches: Dict[str, TradeMatch] = {}
        self._household_surplus: Dict[str, float] = {}   # householdId → surplus kWh
        self._household_deficit: Dict[str, float] = {}   # householdId → deficit kWh
        self._match_count = 0

        # Subscribe to IoT readings (Conformist relationship via event bus)
        self._bus.subscribe(TOPIC_METER_READINGS, self._on_meter_reading, "marketplace-service")
        # Subscribe to settlement outcomes
        self._bus.subscribe(TOPIC_SETTLEMENT, self._on_settlement_event, "marketplace-service")

    # ── Event Handlers ──────────────────────────────────────────────────────

    def _on_meter_reading(self, event: Dict):
        """
        React to MeterReadingReceived events.
        Update known surplus/deficit per household and auto-generate offers.
        This is the Anti-Corruption Layer: translate IoT event to Marketplace model.
        """
        hid = event["householdId"]
        surplus = event["surplusKwh"]
        deficit = event["deficitKwh"]
        self._household_surplus[hid] = surplus
        self._household_deficit[hid] = deficit

        # Auto-generate sell offer if surplus exists
        if surplus > 0.1:
            self.create_offer(hid, "SELL", surplus, price_per_kwh=28.5, grid_zone="ZONE-A")
        # Auto-generate buy offer if deficit exists
        if deficit > 0.1:
            self.create_offer(hid, "BUY", deficit, price_per_kwh=30.0, grid_zone="ZONE-A")

    def _on_settlement_event(self, event: Dict):
        """React to SettlementConfirmed or SettlementFailed events (Saga step)."""
        match_id = event.get("matchId")
        status = event.get("eventType")
        if match_id in self._matches:
            match = self._matches[match_id]
            if status == "SettlementConfirmed":
                match.status = "SETTLED"
                self._log.info("Trade SETTLED | matchId=%s", match_id)
            elif status == "SettlementFailed":
                # Compensating transaction: revert offers
                match.status = "SETTLEMENT_FAILED"
                self._log.warning(
                    "Settlement FAILED for matchId=%s – compensating: offers reverted", match_id
                )

    # ── Domain Operations ────────────────────────────────────────────────────

    def create_offer(self, household_id: str, offer_type: str,
                     kwh_amount: float, price_per_kwh: float, grid_zone: str) -> TradeOffer:
        """Create and register a trade offer."""
        offer = TradeOffer(
            household_id=household_id,
            offer_type=offer_type,
            kwh_amount=kwh_amount,
            price_per_kwh=price_per_kwh,
            grid_zone=grid_zone
        )
        self._offers[offer.offer_id] = offer
        self._log.info(
            "Offer created | type=%s household=%s %.3fkWh @ %.1f¢/kWh",
            offer_type, household_id, kwh_amount, price_per_kwh
        )
        # Attempt matching after each new offer
        self._attempt_matching()
        return offer

    def _attempt_matching(self):
        """
        Matching Engine: pair SELL and BUY offers in the same grid zone.
        Uses a simple price-priority algorithm (lowest sell price, highest buy price).
        """
        open_sells = sorted(
            [o for o in self._offers.values() if o.offer_type == "SELL" and o.status == "OPEN"],
            key=lambda o: o.price_per_kwh
        )
        open_buys = sorted(
            [o for o in self._offers.values() if o.offer_type == "BUY" and o.status == "OPEN"],
            key=lambda o: -o.price_per_kwh
        )

        for sell in open_sells:
            for buy in open_buys:
                if (buy.status != "OPEN" or sell.status != "OPEN"):
                    continue
                if sell.grid_zone != buy.grid_zone:
                    continue
                if sell.household_id == buy.household_id:
                    continue
                # Price agreement: buyer willing to pay >= seller asking price
                if buy.price_per_kwh >= sell.price_per_kwh:
                    kwh_traded = min(sell.kwh_amount, buy.kwh_amount)
                    agreed_price = (sell.price_per_kwh + buy.price_per_kwh) / 2

                    match = TradeMatch(
                        sell_offer_id=sell.offer_id,
                        buy_offer_id=buy.offer_id,
                        seller_id=sell.household_id,
                        buyer_id=buy.household_id,
                        kwh_traded=round(kwh_traded, 4),
                        agreed_price_per_kwh=round(agreed_price, 2)
                    )
                    self._matches[match.match_id] = match
                    sell.status = "MATCHED"
                    buy.status = "MATCHED"
                    self._match_count += 1

                    self._log.info(
                        "MATCH! seller=%s buyer=%s %.3fkWh @ %.2f¢/kWh | matchId=%s",
                        sell.household_id, buy.household_id,
                        kwh_traded, agreed_price, match.match_id
                    )

                    # Publish TradeMatched domain event (triggers Settlement Saga)
                    self._bus.publish(TOPIC_MARKETPLACE, {
                        "eventType": "TradeMatched",
                        "matchId": match.match_id,
                        "sellOfferId": sell.offer_id,
                        "buyOfferId": buy.offer_id,
                        "sellerId": sell.household_id,
                        "buyerId": buy.household_id,
                        "kwhTraded": kwh_traded,
                        "agreedPricePerKwh": agreed_price
                    })

    def get_stats(self) -> Dict:
        open_offers = sum(1 for o in self._offers.values() if o.status == "OPEN")
        return {"total_offers": len(self._offers), "open_offers": open_offers,
                "total_matches": self._match_count}