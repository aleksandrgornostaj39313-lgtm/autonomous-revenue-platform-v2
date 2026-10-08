# MODULE 5: EVENT SOURCING & CQRS
## Immutable event log, read/write model separation, audit trail, compliance storage, and event replay

```python
# src/event_store/models.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class EventType(str, Enum):
    # Deal lifecycle events
    DEAL_CREATED = "deal_created"
    DEAL_QUALIFIED = "deal_qualified"
    DEAL_PROPOSAL_SENT = "deal_proposal_sent"
    DEAL_NEGOTIATION_STARTED = "deal_negotiation_started"
    DEAL_WON = "deal_won"
    DEAL_LOST = "deal_lost"
    DEAL_STUCK = "deal_stuck"
    
    # Activity events
    ACTIVITY_RECORDED = "activity_recorded"
    CALL_TRANSCRIPT_INDEXED = "call_transcript_indexed"
    EMAIL_RECEIVED = "email_received"
    NOTE_ADDED = "note_added"
    
    # AI events
    AI_QUALIFICATION_COMPLETED = "ai_qualification_completed"
    AI_PROPOSAL_GENERATED = "ai_proposal_generated"
    AI_COACHING_PROVIDED = "ai_coaching_provided"
    AI_FORECAST_GENERATED = "ai_forecast_generated"
    
    # CRM sync events
    EXTERNAL_SYNC_STARTED = "external_sync_started"
    EXTERNAL_SYNC_COMPLETED = "external_sync_completed"
    EXTERNAL_SYNC_FAILED = "external_sync_failed"
    CONFLICT_DETECTED = "conflict_detected"
    CONFLICT_RESOLVED = "conflict_resolved"
    
    # Vector store events
    DOCUMENT_INDEXED = "document_indexed"
    MEMORY_UPDATED = "memory_updated"
    KNOWLEDGE_GRAPH_UPDATED = "knowledge_graph_updated"
    
    # Workflow events
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"
    ACTIVITY_EXECUTED = "activity_executed"


class EventAggregateType(str, Enum):
    DEAL = "deal"
    CONTACT = "contact"
    COMPANY = "company"
    WORKFLOW = "workflow"
    SYNC = "sync"


class EventStorageFormat(str, Enum):
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


@dataclass
class DomainEvent:
    """Base domain event class"""
    event_id: str
    event_type: EventType
    aggregate_id: str
    aggregate_type: EventAggregateType
    aggregate_version: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    actor_id: str = "system"
    payload: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    causation_id: Optional[str] = None
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type.value,
            "aggregate_version": self.aggregate_version,
            "timestamp": self.timestamp.isoformat(),
            "actor_id": self.actor_id,
            "payload": self.payload,
            "metadata": self.metadata,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
        }


@dataclass
class SnapshotData:
    """Snapshot of aggregate state at a point in time"""
    snapshot_id: str
    aggregate_id: str
    aggregate_type: EventAggregateType
    aggregate_version: int
    state: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type.value,
            "aggregate_version": self.aggregate_version,
            "state": self.state,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class ReadModel:
    """Denormalized view optimized for queries"""
    model_id: str
    aggregate_id: str
    model_type: str  # "deal_summary", "pipeline_overview", etc.
    data: Dict[str, Any]
    version: int
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ComplianceRecord:
    """Audit and compliance record"""
    record_id: str
    event_id: str
    aggregate_id: str
    change_type: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    actor_id: str = "system"
    reason: Optional[str] = None
    recorded_at: datetime = field(default_factory=datetime.utcnow)
    retention_until: datetime = field(default_factory=lambda: datetime.utcnow() + __import__('datetime').timedelta(days=2555))  # 7 years
```

```python
# src/event_store/event_repository.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
import logging

from src.event_store.models import (
    DomainEvent,
    EventType,
    EventAggregateType,
    SnapshotData,
    ReadModel,
    ComplianceRecord,
)

logger = logging.getLogger(__name__)


class EventRepository(ABC):
    @abstractmethod
    async def append_event(self, event: DomainEvent) -> DomainEvent:
        raise NotImplementedError

    @abstractmethod
    async def get_events_for_aggregate(
        self,
        aggregate_id: str,
        from_version: int = 0,
    ) -> List[DomainEvent]:
        raise NotImplementedError

    @abstractmethod
    async def get_events_by_type(
        self,
        event_type: EventType,
        limit: int = 100,
    ) -> List[DomainEvent]:
        raise NotImplementedError

    @abstractmethod
    async def get_events_after(
        self,
        timestamp: datetime,
        limit: int = 100,
    ) -> List[DomainEvent]:
        raise NotImplementedError


class PostgresEventRepository(EventRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def append_event(self, event: DomainEvent) -> DomainEvent:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO event_store (
                    event_id, event_type, aggregate_id, aggregate_type, aggregate_version,
                    timestamp, actor_id, payload, metadata, causation_id, correlation_id
                ) VALUES (
                    :event_id, :event_type, :aggregate_id, :aggregate_type, :aggregate_version,
                    :timestamp, :actor_id, :payload, :metadata, :causation_id, :correlation_id
                )
                """,
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "aggregate_id": event.aggregate_id,
                    "aggregate_type": event.aggregate_type.value,
                    "aggregate_version": event.aggregate_version,
                    "timestamp": event.timestamp,
                    "actor_id": event.actor_id,
                    "payload": event.payload,
                    "metadata": event.metadata,
                    "causation_id": event.causation_id,
                    "correlation_id": event.correlation_id,
                },
            )
            await session.commit()
        
        logger.info(
            f"Appended event {event.event_id} "
            f"(type={event.event_type.value}, aggregate={event.aggregate_id})"
        )
        return event

    async def get_events_for_aggregate(
        self,
        aggregate_id: str,
        from_version: int = 0,
    ) -> List[DomainEvent]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                """
                SELECT * FROM event_store
                WHERE aggregate_id = :aggregate_id AND aggregate_version > :from_version
                ORDER BY aggregate_version ASC
                """,
                {"aggregate_id": aggregate_id, "from_version": from_version},
            )
        
        events = [
            DomainEvent(
                event_id=row["event_id"],
                event_type=EventType(row["event_type"]),
                aggregate_id=row["aggregate_id"],
                aggregate_type=EventAggregateType(row["aggregate_type"]),
                aggregate_version=row["aggregate_version"],
                timestamp=row["timestamp"],
                actor_id=row["actor_id"],
                payload=row["payload"] or {},
                metadata=row["metadata"] or {},
                causation_id=row["causation_id"],
                correlation_id=row["correlation_id"],
            )
            for row in rows
        ]
        
        logger.info(f"Retrieved {len(events)} events for aggregate {aggregate_id}")
        return events

    async def get_events_by_type(
        self,
        event_type: EventType,
        limit: int = 100,
    ) -> List[DomainEvent]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                """
                SELECT * FROM event_store
                WHERE event_type = :event_type
                ORDER BY timestamp DESC
                LIMIT :limit
                """,
                {"event_type": event_type.value, "limit": limit},
            )
        
        return [
            DomainEvent(
                event_id=row["event_id"],
                event_type=EventType(row["event_type"]),
                aggregate_id=row["aggregate_id"],
                aggregate_type=EventAggregateType(row["aggregate_type"]),
                aggregate_version=row["aggregate_version"],
                timestamp=row["timestamp"],
                actor_id=row["actor_id"],
                payload=row["payload"] or {},
                metadata=row["metadata"] or {},
                causation_id=row["causation_id"],
                correlation_id=row["correlation_id"],
            )
            for row in rows
        ]

    async def get_events_after(
        self,
        timestamp: datetime,
        limit: int = 100,
    ) -> List[DomainEvent]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                """
                SELECT * FROM event_store
                WHERE timestamp > :timestamp
                ORDER BY timestamp ASC
                LIMIT :limit
                """,
                {"timestamp": timestamp, "limit": limit},
            )
        
        return [
            DomainEvent(
                event_id=row["event_id"],
                event_type=EventType(row["event_type"]),
                aggregate_id=row["aggregate_id"],
                aggregate_type=EventAggregateType(row["aggregate_type"]),
                aggregate_version=row["aggregate_version"],
                timestamp=row["timestamp"],
                actor_id=row["actor_id"],
                payload=row["payload"] or {},
                metadata=row["metadata"] or {},
                causation_id=row["causation_id"],
                correlation_id=row["correlation_id"],
            )
            for row in rows
        ]


class SnapshotRepository(ABC):
    @abstractmethod
    async def save_snapshot(self, snapshot: SnapshotData) -> SnapshotData:
        raise NotImplementedError

    @abstractmethod
    async def get_latest_snapshot(
        self,
        aggregate_id: str,
    ) -> Optional[SnapshotData]:
        raise NotImplementedError


class PostgresSnapshotRepository(SnapshotRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def save_snapshot(self, snapshot: SnapshotData) -> SnapshotData:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO snapshots (
                    snapshot_id, aggregate_id, aggregate_type, aggregate_version,
                    state, created_at
                ) VALUES (
                    :snapshot_id, :aggregate_id, :aggregate_type, :aggregate_version,
                    :state, :created_at
                )
                """,
                {
                    "snapshot_id": snapshot.snapshot_id,
                    "aggregate_id": snapshot.aggregate_id,
                    "aggregate_type": snapshot.aggregate_type.value,
                    "aggregate_version": snapshot.aggregate_version,
                    "state": snapshot.state,
                    "created_at": snapshot.created_at,
                },
            )
            await session.commit()
        
        logger.info(f"Saved snapshot for aggregate {snapshot.aggregate_id}")
        return snapshot

    async def get_latest_snapshot(
        self,
        aggregate_id: str,
    ) -> Optional[SnapshotData]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                """
                SELECT * FROM snapshots
                WHERE aggregate_id = :aggregate_id
                ORDER BY aggregate_version DESC
                LIMIT 1
                """,
                {"aggregate_id": aggregate_id},
            )
        
        if row is None:
            return None
        
        return SnapshotData(
            snapshot_id=row["snapshot_id"],
            aggregate_id=row["aggregate_id"],
            aggregate_type=EventAggregateType(row["aggregate_type"]),
            aggregate_version=row["aggregate_version"],
            state=row["state"],
            created_at=row["created_at"],
        )


class ReadModelRepository(ABC):
    @abstractmethod
    async def save_read_model(self, model: ReadModel) -> ReadModel:
        raise NotImplementedError

    @abstractmethod
    async def get_read_model(
        self,
        aggregate_id: str,
        model_type: str,
    ) -> Optional[ReadModel]:
        raise NotImplementedError

    @abstractmethod
    async def query_read_models(
        self,
        model_type: str,
        filters: Dict[str, Any],
    ) -> List[ReadModel]:
        raise NotImplementedError


class PostgresReadModelRepository(ReadModelRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def save_read_model(self, model: ReadModel) -> ReadModel:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO read_models (
                    model_id, aggregate_id, model_type, data, version, updated_at
                ) VALUES (
                    :model_id, :aggregate_id, :model_type, :data, :version, :updated_at
                )
                ON CONFLICT (aggregate_id, model_type) DO UPDATE SET
                    data = EXCLUDED.data,
                    version = EXCLUDED.version,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "model_id": model.model_id,
                    "aggregate_id": model.aggregate_id,
                    "model_type": model.model_type,
                    "data": model.data,
                    "version": model.version,
                    "updated_at": model.updated_at,
                },
            )
            await session.commit()
        
        return model

    async def get_read_model(
        self,
        aggregate_id: str,
        model_type: str,
    ) -> Optional[ReadModel]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                """
                SELECT * FROM read_models
                WHERE aggregate_id = :aggregate_id AND model_type = :model_type
                """,
                {"aggregate_id": aggregate_id, "model_type": model_type},
            )
        
        if row is None:
            return None
        
        return ReadModel(
            model_id=row["model_id"],
            aggregate_id=row["aggregate_id"],
            model_type=row["model_type"],
            data=row["data"],
            version=row["version"],
            updated_at=row["updated_at"],
        )

    async def query_read_models(
        self,
        model_type: str,
        filters: Dict[str, Any],
    ) -> List[ReadModel]:
        async with self.session_factory() as session:
            where_clauses = ["model_type = :model_type"]
            params = {"model_type": model_type}
            
            for key, value in filters.items():
                where_clauses.append(f"data->'{key}' = :{key}")
                params[key] = value
            
            where_sql = " AND ".join(where_clauses)
            
            rows = await session.fetch_all(
                f"""
                SELECT * FROM read_models
                WHERE {where_sql}
                ORDER BY updated_at DESC
                """,
                params,
            )
        
        return [
            ReadModel(
                model_id=row["model_id"],
                aggregate_id=row["aggregate_id"],
                model_type=row["model_type"],
                data=row["data"],
                version=row["version"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]


class ComplianceRepository(ABC):
    @abstractmethod
    async def record_change(self, record: ComplianceRecord) -> ComplianceRecord:
        raise NotImplementedError

    @abstractmethod
    async def get_audit_trail(
        self,
        aggregate_id: str,
    ) -> List[ComplianceRecord]:
        raise NotImplementedError


class PostgresComplianceRepository(ComplianceRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def record_change(self, record: ComplianceRecord) -> ComplianceRecord:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO compliance_records (
                    record_id, event_id, aggregate_id, change_type, old_value,
                    new_value, actor_id, reason, recorded_at, retention_until
                ) VALUES (
                    :record_id, :event_id, :aggregate_id, :change_type, :old_value,
                    :new_value, :actor_id, :reason, :recorded_at, :retention_until
                )
                """,
                {
                    "record_id": record.record_id,
                    "event_id": record.event_id,
                    "aggregate_id": record.aggregate_id,
                    "change_type": record.change_type,
                    "old_value": record.old_value,
                    "new_value": record.new_value,
                    "actor_id": record.actor_id,
                    "reason": record.reason,
                    "recorded_at": record.recorded_at,
                    "retention_until": record.retention_until,
                },
            )
            await session.commit()
        
        return record

    async def get_audit_trail(
        self,
        aggregate_id: str,
    ) -> List[ComplianceRecord]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                """
                SELECT * FROM compliance_records
                WHERE aggregate_id = :aggregate_id
                ORDER BY recorded_at ASC
                """,
                {"aggregate_id": aggregate_id},
            )
        
        return [
            ComplianceRecord(
                record_id=row["record_id"],
                event_id=row["event_id"],
                aggregate_id=row["aggregate_id"],
                change_type=row["change_type"],
                old_value=row["old_value"],
                new_value=row["new_value"],
                actor_id=row["actor_id"],
                reason=row["reason"],
                recorded_at=row["recorded_at"],
                retention_until=row["retention_until"],
            )
            for row in rows
        ]
```

```python
# src/event_store/event_publisher.py

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from src.event_store.models import DomainEvent, EventType

logger = logging.getLogger(__name__)


class EventPublisher(ABC):
    @abstractmethod
    async def publish(self, event: DomainEvent) -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe(
        self,
        event_type: EventType,
        handler: Callable[[DomainEvent], Any],
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> None:
        raise NotImplementedError


class InMemoryEventPublisher(EventPublisher):
    """Simple in-memory event publisher for local development"""
    
    def __init__(self):
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.subscription_counter = 0

    async def publish(self, event: DomainEvent) -> None:
        event_type_value = event.event_type.value
        
        if event_type_value in self.subscriptions:
            for handler in self.subscriptions[event_type_value]:
                try:
                    await handler(event) if hasattr(handler, "__await__") else handler(event)
                except Exception as exc:
                    logger.exception(f"Handler error for {event_type_value}: {exc}")

    async def subscribe(
        self,
        event_type: EventType,
        handler: Callable[[DomainEvent], Any],
    ) -> str:
        event_type_value = event_type.value
        if event_type_value not in self.subscriptions:
            self.subscriptions[event_type_value] = []
        
        self.subscriptions[event_type_value].append(handler)
        
        subscription_id = f"sub-{self.subscription_counter}"
        self.subscription_counter += 1
        
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> None:
        for handlers in self.subscriptions.values():
            handlers.clear()


class KafkaEventPublisher(EventPublisher):
    """Kafka-backed event publisher for production"""
    
    def __init__(self, bootstrap_servers: List[str], topic_prefix: str = "domain-events"):
        try:
            from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
        except ImportError:
            raise ImportError("aiokafka library required for Kafka support")
        
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumers: Dict[str, AIOKafkaConsumer] = {}
        self.subscriptions: Dict[str, List[Callable]] = {}
    
    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: __import__("json").dumps(v).encode("utf-8"),
        )
        await self.producer.start()
        logger.info("Kafka producer started")
    
    async def stop(self) -> None:
        if self.producer:
            await self.producer.stop()
        for consumer in self.consumers.values():
            await consumer.stop()
        logger.info("Kafka clients stopped")
    
    async def publish(self, event: DomainEvent) -> None:
        if not self.producer:
            raise RuntimeError("Producer not started")
        
        topic = f"{self.topic_prefix}.{event.event_type.value}"
        
        await self.producer.send_and_wait(
            topic,
            value=event.to_dict(),
            key=event.aggregate_id.encode("utf-8"),
        )
        
        logger.info(f"Published event to {topic}: {event.event_id}")
    
    async def subscribe(
        self,
        event_type: EventType,
        handler: Callable[[DomainEvent], Any],
    ) -> str:
        from aiokafka import AIOKafkaConsumer
        
        topic = f"{self.topic_prefix}.{event_type.value}"
        
        if topic not in self.consumers:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: __import__("json").loads(m.decode("utf-8")),
                group_id=f"platform-{event_type.value}",
                auto_offset_reset="earliest",
            )
            self.consumers[topic] = consumer
            await consumer.start()
            
            if topic not in self.subscriptions:
                self.subscriptions[topic] = []
        
        self.subscriptions[topic].append(handler)
        
        return f"sub-{topic}-{len(self.subscriptions[topic])}"
    
    async def unsubscribe(self, subscription_id: str) -> None:
        for handlers in self.subscriptions.values():
            handlers.clear()
```

```python
# src/event_store/event_sourcing_service.py

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.event_store.event_publisher import EventPublisher
from src.event_store.event_repository import (
    EventRepository,
    SnapshotRepository,
    ReadModelRepository,
    ComplianceRepository,
)
from src.event_store.models import (
    DomainEvent,
    EventType,
    EventAggregateType,
    SnapshotData,
    ReadModel,
    ComplianceRecord,
)

logger = logging.getLogger(__name__)


class AggregateRoot:
    """Base class for domain aggregates with event sourcing"""
    
    def __init__(self, aggregate_id: str, aggregate_type: EventAggregateType):
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.version = 0
        self.changes: List[DomainEvent] = []
    
    def apply_event(self, event: DomainEvent) -> None:
        """Apply an event to the aggregate"""
        self.version = event.aggregate_version
        
        # Dispatch to appropriate handler
        handler_name = f"on_{event.event_type.value}"
        if hasattr(self, handler_name):
            getattr(self, handler_name)(event)
    
    def record_event(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        actor_id: str = "system",
        causation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> DomainEvent:
        """Record a new event"""
        self.version += 1
        
        event = DomainEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.version,
            actor_id=actor_id,
            payload=payload,
            causation_id=causation_id,
            correlation_id=correlation_id,
        )
        
        self.changes.append(event)
        self.apply_event(event)
        
        return event
    
    def get_uncommitted_changes(self) -> List[DomainEvent]:
        return self.changes
    
    def clear_changes(self) -> None:
        self.changes.clear()


class DealAggregate(AggregateRoot):
    """Deal aggregate with event sourcing"""
    
    def __init__(self, deal_id: str):
        super().__init__(deal_id, EventAggregateType.DEAL)
        self.stage = "lead"
        self.company_id: Optional[str] = None
        self.amount = 0.0
        self.probability = 0.0
        self.owner_id: Optional[str] = None
        self.expected_close_date: Optional[datetime] = None
    
    def on_deal_created(self, event: DomainEvent) -> None:
        self.stage = "lead"
        self.company_id = event.payload.get("company_id")
        self.amount = event.payload.get("amount", 0.0)
        self.probability = event.payload.get("probability", 0.0)
        self.owner_id = event.payload.get("owner_id")
        self.expected_close_date = event.payload.get("expected_close_date")
    
    def on_deal_qualified(self, event: DomainEvent) -> None:
        self.stage = "qualified"
        self.probability = event.payload.get("probability", self.probability)
    
    def on_deal_proposal_sent(self, event: DomainEvent) -> None:
        self.stage = "proposal_sent"
    
    def on_deal_negotiation_started(self, event: DomainEvent) -> None:
        self.stage = "negotiation"
    
    def on_deal_won(self, event: DomainEvent) -> None:
        self.stage = "won"
    
    def on_deal_lost(self, event: DomainEvent) -> None:
        self.stage = "lost"
    
    def on_deal_stuck(self, event: DomainEvent) -> None:
        self.stage = "stuck"


class EventSourcingService:
    """Main service for event sourcing operations"""
    
    SNAPSHOT_INTERVAL = 10  # Create snapshot every 10 events
    
    def __init__(
        self,
        event_repository: EventRepository,
        snapshot_repository: SnapshotRepository,
        read_model_repository: ReadModelRepository,
        compliance_repository: ComplianceRepository,
        event_publisher: EventPublisher,
    ):
        self.event_repo = event_repository
        self.snapshot_repo = snapshot_repository
        self.read_model_repo = read_model_repository
        self.compliance_repo = compliance_repository
        self.publisher = event_publisher
    
    async def save_aggregate(
        self,
        aggregate: AggregateRoot,
    ) -> None:
        """Save aggregate changes as events"""
        
        changes = aggregate.get_uncommitted_changes()
        
        for event in changes:
            await self.event_repo.append_event(event)
            await self.publisher.publish(event)
            
            logger.info(
                f"Saved event {event.event_id} "
                f"(aggregate={event.aggregate_id}, version={event.aggregate_version})"
            )
        
        # Create snapshot if threshold reached
        if aggregate.version % self.SNAPSHOT_INTERVAL == 0:
            await self._create_snapshot(aggregate)
        
        aggregate.clear_changes()
    
    async def load_aggregate(
        self,
        aggregate_id: str,
        aggregate_type: EventAggregateType,
    ) -> AggregateRoot:
        """Load aggregate from event store"""
        
        # Load latest snapshot if available
        snapshot = await self.snapshot_repo.get_latest_snapshot(aggregate_id)
        
        aggregate: AggregateRoot
        if aggregate_type == EventAggregateType.DEAL:
            aggregate = DealAggregate(aggregate_id)
        else:
            raise ValueError(f"Unsupported aggregate type: {aggregate_type}")
        
        # Restore from snapshot if available
        if snapshot:
            aggregate.version = snapshot.aggregate_version
            aggregate.stage = snapshot.state.get("stage")
            aggregate.company_id = snapshot.state.get("company_id")
            aggregate.amount = snapshot.state.get("amount", 0.0)
            aggregate.probability = snapshot.state.get("probability", 0.0)
            aggregate.owner_id = snapshot.state.get("owner_id")
            aggregate.expected_close_date = snapshot.state.get("expected_close_date")
            
            from_version = snapshot.aggregate_version
        else:
            from_version = 0
        
        # Replay events after snapshot
        events = await self.event_repo.get_events_for_aggregate(aggregate_id, from_version)
        
        for event in events:
            aggregate.apply_event(event)
        
        logger.info(
            f"Loaded aggregate {aggregate_id} "
            f"(version={aggregate.version}, events={len(events)})"
        )
        
        return aggregate
    
    async def _create_snapshot(self, aggregate: AggregateRoot) -> None:
        """Create a snapshot of aggregate state"""
        
        state_dict = {
            "stage": aggregate.stage if hasattr(aggregate, "stage") else None,
            "company_id": aggregate.company_id if hasattr(aggregate, "company_id") else None,
            "amount": aggregate.amount if hasattr(aggregate, "amount") else 0.0,
            "probability": aggregate.probability if hasattr(aggregate, "probability") else 0.0,
            "owner_id": aggregate.owner_id if hasattr(aggregate, "owner_id") else None,
            "expected_close_date": aggregate.expected_close_date if hasattr(aggregate, "expected_close_date") else None,
        }
        
        snapshot = SnapshotData(
            snapshot_id=str(uuid.uuid4()),
            aggregate_id=aggregate.aggregate_id,
            aggregate_type=aggregate.aggregate_type,
            aggregate_version=aggregate.version,
            state=state_dict,
        )
        
        await self.snapshot_repo.save_snapshot(snapshot)
        logger.info(f"Created snapshot for {aggregate.aggregate_id}")
    
    async def record_compliance_change(
        self,
        event: DomainEvent,
        old_value: Optional[Any] = None,
        new_value: Optional[Any] = None,
        reason: Optional[str] = None,
    ) -> ComplianceRecord:
        """Record a compliance audit record"""
        
        record = ComplianceRecord(
            record_id=str(uuid.uuid4()),
            event_id=event.event_id,
            aggregate_id=event.aggregate_id,
            change_type=event.event_type.value,
            old_value=old_value,
            new_value=new_value,
            actor_id=event.actor_id,
            reason=reason,
        )
        
        return await self.compliance_repo.record_change(record)
    
    async def get_audit_trail(self, aggregate_id: str) -> List[Dict[str, Any]]:
        """Get full audit trail for an aggregate"""
        
        compliance_records = await self.compliance_repo.get_audit_trail(aggregate_id)
        events = await self.event_repo.get_events_for_aggregate(aggregate_id)
        
        trail = []
        for event in events:
            trail.append({
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "timestamp": event.timestamp.isoformat(),
                "actor": event.actor_id,
                "version": event.aggregate_version,
                "payload": event.payload,
            })
        
        for record in compliance_records:
            trail.append({
                "record_id": record.record_id,
                "change_type": record.change_type,
                "timestamp": record.recorded_at.isoformat(),
                "actor": record.actor_id,
                "old_value": record.old_value,
                "new_value": record.new_value,
                "reason": record.reason,
            })
        
        trail.sort(key=lambda x: x.get("timestamp", ""))
        
        return trail
```

```python
# src/event_store/read_model_projector.py

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from src.event_store.models import DomainEvent, EventType
from src.event_store.event_repository import ReadModelRepository

logger = logging.getLogger(__name__)


class ReadModelProjector:
    """Projects events into denormalized read models"""
    
    def __init__(self, read_model_repo: ReadModelRepository):
        self.read_model_repo = read_model_repo
    
    async def project_deal_summary(self, event: DomainEvent) -> None:
        """Project deal events into a summary read model"""
        
        # Get existing read model
        existing = await self.read_model_repo.get_read_model(
            event.aggregate_id,
            "deal_summary",
        )
        
        if existing:
            data = existing.data
            version = existing.version + 1
        else:
            data = {
                "deal_id": event.aggregate_id,
                "stage": "lead",
                "amount": 0.0,
                "probability": 0.0,
            }
            version = 1
        
        # Apply event transformations
        if event.event_type == EventType.DEAL_CREATED:
            data.update({
                "stage": "lead",
                "amount": event.payload.get("amount", 0.0),
                "probability": event.payload.get("probability", 0.0),
                "company_id": event.payload.get("company_id"),
                "owner_id": event.payload.get("owner_id"),
            })
        
        elif event.event_type == EventType.DEAL_QUALIFIED:
            data["stage"] = "qualified"
            data["probability"] = event.payload.get("probability", data["probability"])
        
        elif event.event_type == EventType.DEAL_PROPOSAL_SENT:
            data["stage"] = "proposal_sent"
        
        elif event.event_type == EventType.DEAL_NEGOTIATION_STARTED:
            data["stage"] = "negotiation"
        
        elif event.event_type == EventType.DEAL_WON:
            data["stage"] = "won"
        
        elif event.event_type == EventType.DEAL_LOST:
            data["stage"] = "lost"
        
        elif event.event_type == EventType.DEAL_STUCK:
            data["stage"] = "stuck"
        
        # Save read model
        from src.event_store.models import ReadModel
        import uuid
        
        read_model = ReadModel(
            model_id=str(uuid.uuid4()),
            aggregate_id=event.aggregate_id,
            model_type="deal_summary",
            data=data,
            version=version,
        )
        
        await self.read_model_repo.save_read_model(read_model)
        
        logger.info(f"Projected deal summary for {event.aggregate_id}")
    
    async def project_pipeline_overview(self, event: DomainEvent) -> None:
        """Project deal events into pipeline statistics"""
        
        if event.event_type not in {
            EventType.DEAL_CREATED,
            EventType.DEAL_QUALIFIED,
            EventType.DEAL_PROPOSAL_SENT,
            EventType.DEAL_NEGOTIATION_STARTED,
            EventType.DEAL_WON,
            EventType.DEAL_LOST,
        }:
            return
        
        # In production, this would aggregate multiple deals
        # For now, we track a simple stage count
        
        pipeline_id = "global_pipeline"
        
        existing = await self.read_model_repo.get_read_model(
            pipeline_id,
            "pipeline_overview",
        )
        
        if existing:
            data = existing.data
            version = existing.version + 1
        else:
            data = {
                "total_deals": 0,
                "stages": {
                    "lead": 0,
                    "qualified": 0,
                    "proposal_sent": 0,
                    "negotiation": 0,
                    "won": 0,
                    "lost": 0,
                },
                "total_pipeline_value": 0.0,
            }
            version = 1
        
        # Update based on event
        if event.event_type == EventType.DEAL_CREATED:
            data["total_deals"] = data.get("total_deals", 0) + 1
            data["stages"]["lead"] = data["stages"].get("lead", 0) + 1
            data["total_pipeline_value"] = data.get("total_pipeline_value", 0.0) + event.payload.get("amount", 0.0)
        
        elif event.event_type == EventType.DEAL_QUALIFIED:
            data["stages"]["lead"] = max(0, data["stages"].get("lead", 1) - 1)
            data["stages"]["qualified"] = data["stages"].get("qualified", 0) + 1
        
        from src.event_store.models import ReadModel
        import uuid
        
        read_model = ReadModel(
            model_id=str(uuid.uuid4()),
            aggregate_id=pipeline_id,
            model_type="pipeline_overview",
            data=data,
            version=version,
        )
        
        await self.read_model_repo.save_read_model(read_model)
```

```python
# src/event_store/event_replay_engine.py

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from src.event_store.event_repository import EventRepository
from src.event_store.models import DomainEvent, EventAggregateType

logger = logging.getLogger(__name__)


class EventReplayEngine:
    """Replay events to rebuild aggregate state or debug issues"""
    
    def __init__(self, event_repository: EventRepository):
        self.event_repo = event_repository
    
    async def replay_aggregate_history(
        self,
        aggregate_id: str,
        up_to_version: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Replay entire history of an aggregate"""
        
        events = await self.event_repo.get_events_for_aggregate(aggregate_id)
        
        history = []
        for event in events:
            if up_to_version and event.aggregate_version > up_to_version:
                break
            
            history.append({
                "version": event.aggregate_version,
                "event_type": event.event_type.value,
                "timestamp": event.timestamp.isoformat(),
                "actor": event.actor_id,
                "payload": event.payload,
            })
        
        logger.info(f"Replayed {len(history)} events for {aggregate_id}")
        
        return history
    
    async def replay_to_specific_time(
        self,
        aggregate_id: str,
        target_time: datetime,
    ) -> List[DomainEvent]:
        """Replay events up to a specific point in time"""
        
        events = await self.event_repo.get_events_for_aggregate(aggregate_id)
        
        relevant_events = [
            event for event in events
            if event.timestamp <= target_time
        ]
        
        logger.info(
            f"Replayed {len(relevant_events)} events for {aggregate_id} "
            f"up to {target_time.isoformat()}"
        )
        
        return relevant_events


from typing import Any, Dict
```

```python
# src/event_store/integration_with_modules.py

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict

from src.event_store.event_sourcing_service import (
    EventSourcingService,
    DealAggregate,
)
from src.event_store.models import EventType, EventAggregateType
from src.event_store.read_model_projector import ReadModelProjector

logger = logging.getLogger(__name__)


class EventSourcingIntegration:
    """Integration point between event sourcing and other modules"""
    
    def __init__(
        self,
        event_sourcing_service: EventSourcingService,
        read_model_projector: ReadModelProjector,
    ):
        self.event_service = event_sourcing_service
        self.projector = read_model_projector
    
    async def record_deal_created_from_crm(
        self,
        deal_id: str,
        company_id: str,
        owner_id: str,
        amount: float,
        probability: float,
        actor_id: str,
    ) -> str:
        """Record deal creation event from CRM module"""
        
        aggregate = DealAggregate(deal_id)
        
        event = aggregate.record_event(
            event_type=EventType.DEAL_CREATED,
            payload={
                "company_id": company_id,
                "owner_id": owner_id,
                "amount": amount,
                "probability": probability,
            },
            actor_id=actor_id,
            correlation_id=str(uuid.uuid4()),
        )
        
        await self.event_service.save_aggregate(aggregate)
        await self.projector.project_deal_summary(event)
        await self.projector.project_pipeline_overview(event)
        
        logger.info(f"Recorded DEAL_CREATED event for {deal_id}")
        
        return event.event_id
    
    async def record_deal_qualified_from_ai(
        self,
        deal_id: str,
        new_probability: float,
        reasoning: str,
        actor_id: str,
        causation_id: str,
    ) -> str:
        """Record deal qualification from AI module"""
        
        aggregate = await self.event_service.load_aggregate(
            deal_id,
            EventAggregateType.DEAL,
        )
        
        event = aggregate.record_event(
            event_type=EventType.DEAL_QUALIFIED,
            payload={
                "probability": new_probability,
                "reasoning": reasoning,
            },
            actor_id=actor_id,
            causation_id=causation_id,
        )
        
        await self.event_service.save_aggregate(aggregate)
        await self.projector.project_deal_summary(event)
        await self.projector.project_pipeline_overview(event)
        
        # Record compliance change
        await self.event_service.record_compliance_change(
            event,
            old_value={"stage": "lead"},
            new_value={"stage": "qualified", "probability": new_probability},
            reason=reasoning,
        )
        
        logger.info(f"Recorded DEAL_QUALIFIED event for {deal_id}")
        
        return event.event_id
    
    async def record_proposal_sent_from_workflow(
        self,
        deal_id: str,
        proposal_id: str,
        actor_id: str,
        causation_id: str,
    ) -> str:
        """Record proposal sent from workflow orchestration module"""
        
        aggregate = await self.event_service.load_aggregate(
            deal_id,
            EventAggregateType.DEAL,
        )
        
        event = aggregate.record_event(
            event_type=EventType.DEAL_PROPOSAL_SENT,
            payload={
                "proposal_id": proposal_id,
            },
            actor_id=actor_id,
            causation_id=causation_id,
        )
        
        await self.event_service.save_aggregate(aggregate)
        await self.projector.project_deal_summary(event)
        
        logger.info(f"Recorded DEAL_PROPOSAL_SENT event for {deal_id}")
        
        return event.event_id
    
    async def record_external_sync_event(
        self,
        deal_id: str,
        sync_source: str,
        sync_status: str,
        external_id: str,
        actor_id: str,
    ) -> str:
        """Record external CRM sync event"""
        
        event_type = (
            EventType.EXTERNAL_SYNC_COMPLETED
            if sync_status == "success"
            else EventType.EXTERNAL_SYNC_FAILED
        )
        
        event = DealAggregate(deal_id).record_event(
            event_type=event_type,
            payload={
                "sync_source": sync_source,
                "external_id": external_id,
                "status": sync_status,
            },
            actor_id=actor_id,
        )
        
        logger.info(f"Recorded {event_type.value} event for {deal_id}")
        
        return event.event_id
    
    async def record_ai_coaching_event(
        self,
        deal_id: str,
        recommendation: str,
        win_probability: float,
        actor_id: str,
    ) -> str:
        """Record AI coaching recommendation event from Module 2"""
        
        aggregate = await self.event_service.load_aggregate(
            deal_id,
            EventAggregateType.DEAL,
        )
        
        event = aggregate.record_event(
            event_type=EventType.AI_COACHING_PROVIDED,
            payload={
                "recommendation": recommendation,
                "win_probability": win_probability,
            },
            actor_id=actor_id,
        )
        
        await self.event_service.save_aggregate(aggregate)
        
        logger.info(f"Recorded AI_COACHING_PROVIDED event for {deal_id}")
        
        return event.event_id
    
    async def record_call_transcript_indexed(
        self,
        deal_id: str,
        call_id: str,
        transcript_summary: str,
        actor_id: str,
    ) -> str:
        """Record call transcript indexing from Module 4 (Vector Store)"""
        
        aggregate = await self.event_service.load_aggregate(
            deal_id,
            EventAggregateType.DEAL,
        )
        
        event = aggregate.record_event(
            event_type=EventType.CALL_TRANSCRIPT_INDEXED,
            payload={
                "call_id": call_id,
                "transcript_summary": transcript_summary,
            },
            actor_id=actor_id,
        )
        
        await self.event_service.save_aggregate(aggregate)
        
        logger.info(f"Recorded CALL_TRANSCRIPT_INDEXED event for {deal_id}")
        
        return event.event_id
```

---

## CQRS Pattern Implementation

```python
# src/event_store/cqrs_service.py

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from src.event_store.event_repository import ReadModelRepository

logger = logging.getLogger(__name__)


class CQRSQueryService:
    """Command Query Responsibility Separation - Query side"""
    
    def __init__(self, read_model_repo: ReadModelRepository):
        self.read_model_repo = read_model_repo
    
    async def get_deal_summary(self, deal_id: str) -> Optional[Dict[str, Any]]:
        """Query deal summary read model"""
        
        model = await self.read_model_repo.get_read_model(deal_id, "deal_summary")
        
        if model is None:
            return None
        
        return model.data
    
    async def get_pipeline_overview(self) -> Optional[Dict[str, Any]]:
        """Query pipeline overview read model"""
        
        model = await self.read_model_repo.get_read_model("global_pipeline", "pipeline_overview")
        
        if model is None:
            return None
        
        return model.data
    
    async def search_deals_by_stage(self, stage: str) -> List[Dict[str, Any]]:
        """Search deals in a specific stage"""
        
        models = await self.read_model_repo.query_read_models(
            model_type="deal_summary",
            filters={"stage": stage},
        )
        
        return [model.data for model in models]
    
    async def get_high_value_deals(self, min_amount: float) -> List[Dict[str, Any]]:
        """Get all high-value deals above a threshold"""
        
        models = await self.read_model_repo.query_read_models(
            model_type="deal_summary",
            filters={"amount": min_amount},
        )
        
        # Filter client-side since PostgreSQL JSON filtering has limits
        high_value = [
            model.data for model in models
            if model.data.get("amount", 0.0) >= min_amount
        ]
        
        return high_value
```

---

## Architecture Summary: Module 5

Event Sourcing & CQRS is the **source of truth and audit foundation**:

1. **Event Log**: Immutable, append-only ledger of all domain events
2. **Snapshots**: Performance optimization for large aggregates
3. **Read Models**: Denormalized projections optimized for queries
4. **Compliance**: Full audit trail with actor, reason, and timestamps
5. **Event Replay**: Ability to reconstruct state at any point in time
6. **CQRS**: Clean separation of command (write) and query (read) sides

Every module (1–4, 6+) integrates by publishing and subscribing to domain events, making the system eventually consistent, auditable, and debuggable.

---

## Integration Flow Across Modules

```
Module 1 (Workflow)
    ↓
  records command (e.g., "move to proposal")
    ↓
Module 5 (Event Store)
    ├→ appends event to log
    ├→ publishes event
    ├→ updates read model
    └→ records compliance change
    ↓
Module 2 (AI) subscribes
    ↓
  generates coaching
    ↓
Module 5 records AI_COACHING_PROVIDED
    ↓
Module 3 (CRM) subscribes
    ↓
  syncs to external systems
    ↓
Module 5 records EXTERNAL_SYNC_COMPLETED
    ↓
Module 4 (Vector Store) subscribes
    ↓
  indexes transcript/email
    ↓
Module 5 records DOCUMENT_INDEXED
```

All modules are decoupled through event publishing; the system is eventually consistent and fully auditable.

