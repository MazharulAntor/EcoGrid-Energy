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