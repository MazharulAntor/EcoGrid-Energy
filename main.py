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