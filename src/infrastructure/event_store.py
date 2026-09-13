"""
Event Store Implementation for Event Sourcing Pattern
PostgreSQL-based immutable event log with snapshots
"""

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncio
from abc import ABC, abstractmethod

import asyncpg
from sqlalchemy import text, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Float, JSON

from src.core.models import (
    DealEvent, DealSnapshot, Deal, DealStatus
)

logger = logging.getLogger(__name__)

Base = declarative_base()


# ============================================================================
# DATABASE MODELS
# ============================================================================

class EventStoreRecord:
    """ORM Model for event store - immutable audit log"""
    __tablename__ = 'event_store'
    
    event_id = Column(String(36), primary_key=True)
    deal_id = Column(String(36), index=True, nullable=False)
    event_type = Column(String(100), index=True, nullable=False)
    event_data = Column(JSON, nullable=False)
    actor = Column(String(100), default='SYSTEM')
    timestamp = Column(DateTime, index=True, default=datetime.utcnow)
    version = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class SnapshotRecord:
    """ORM Model for snapshots - optimization for fast replay"""
    __tablename__ = 'deal_snapshots'
    
    snapshot_id = Column(String(36), primary_key=True)
    deal_id = Column(String(36), index=True, unique=True, nullable=False)
    status = Column(String(50), nullable=False)
    current_state = Column(JSON, nullable=False)
    version = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# ============================================================================
# EVENT STORE INTERFACE
# ============================================================================

class IEventStore(ABC):
    """Интерфейс для Event Store"""

    @abstractmethod
    async def append_event(self, event: DealEvent) -> bool:
        """Добавить событие в неизменяемый журнал"""
        pass

    @abstractmethod
    async def get_events(
        self, 
        deal_id: str,
        from_version: int = 0,
        to_version: Optional[int] = None
    ) -> List[DealEvent]:
        """Получить события по диапазону версий"""
        pass

    @abstractmethod
    async def get_events_since(
        self,
        deal_id: str,
        timestamp: datetime
    ) -> List[DealEvent]:
        """Получить события после определённого времени"""
        pass

    @abstractmethod
    async def save_snapshot(self, snapshot: DealSnapshot) -> bool:
        """Сохранить снимок состояния"""
        pass

    @abstractmethod
    async def get_latest_snapshot(self, deal_id: str) -> Optional[DealSnapshot]:
        """Получить последний снимок"""
        pass

    @abstractmethod
    async def rebuild_deal_state(
        self,
        deal_id: str,
        snapshot: Optional[DealSnapshot] = None
    ) -> Deal:
        """Восстановить состояние сделки через replay событий"""
        pass

    @abstractmethod
    async def get_all_events(self, deal_id: str) -> List[DealEvent]:
        """Получить все события сделки"""
        pass


# ============================================================================
# POSTGRESQL EVENT STORE IMPLEMENTATION
# ============================================================================

class PostgreSQLEventStore(IEventStore):
    """
    Production-ready Event Store на PostgreSQL
    - Immutable event log
    - Snapshot support для оптимизации
    - ACID transactions
    - CDC (Change Data Capture) ready
    """

    def __init__(
        self,
        database_url: str,
        pool_size: int = 20,
        max_overflow: int = 0,
        snapshot_interval: int = 50  # Снимок каждые N событий
    ):
        """
        Args:
            database_url: PostgreSQL connection URL (async)
            pool_size: Connection pool size
            max_overflow: Max overflow connections
            snapshot_interval: Количество событий между снимками
        """
        self.database_url = database_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.snapshot_interval = snapshot_interval
        
        self.engine = None
        self.async_session_maker = None
        self._initialized = False

    async def initialize(self):
        """Инициализация подключения и создание таблиц"""
        try:
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                connect_args={
                    "timeout": 10,
                    "server_settings": {
                        "application_name": "autonomous_revenue_platform_v2"
                    }
                }
            )
            
            self.async_session_maker = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            await self._create_tables()
            self._initialized = True
            logger.info("Event Store initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Event Store: {str(e)}")
            raise

    async def _create_tables(self):
        """Создание таблиц при необходимости"""
        async with self.engine.begin() as conn:
            # Event Store table
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS event_store (
                    event_id VARCHAR(36) PRIMARY KEY,
                    deal_id VARCHAR(36) NOT NULL,
                    event_type VARCHAR(100) NOT NULL,
                    event_data JSONB NOT NULL,
                    actor VARCHAR(100) DEFAULT 'SYSTEM',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    version INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_deal_id UNIQUE(deal_id, version)
                );
                
                CREATE INDEX IF NOT EXISTS idx_event_store_deal_id ON event_store(deal_id);
                CREATE INDEX IF NOT EXISTS idx_event_store_timestamp ON event_store(timestamp);
                CREATE INDEX IF NOT EXISTS idx_event_store_type ON event_store(event_type);
                CREATE INDEX IF NOT EXISTS idx_event_store_deal_version ON event_store(deal_id, version);
            """))
            
            # Snapshots table
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS deal_snapshots (
                    snapshot_id VARCHAR(36) PRIMARY KEY,
                    deal_id VARCHAR(36) NOT NULL UNIQUE,
                    status VARCHAR(50) NOT NULL,
                    current_state JSONB NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (deal_id) REFERENCES event_store(deal_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_snapshot_deal_id ON deal_snapshots(deal_id);
                CREATE INDEX IF NOT EXISTS idx_snapshot_version ON deal_snapshots(version);
            """))
            
            # Event projections table (для CQRS)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS deal_projections (
                    deal_id VARCHAR(36) PRIMARY KEY,
                    status VARCHAR(50) NOT NULL,
                    company_name VARCHAR(255),
                    estimated_value DECIMAL(15, 2),
                    proposal_id VARCHAR(36),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (deal_id) REFERENCES event_store(deal_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_projection_status ON deal_projections(status);
                CREATE INDEX IF NOT EXISTS idx_projection_updated ON deal_projections(updated_at);
            """))

    async def append_event(self, event: DealEvent) -> bool:
        """
        Добавить событие в неизменяемый журнал
        
        Гарантии:
        - Immutability: событие никогда не изменится
        - Ordering: версия инкрементируется
        - Atomicity: вся операция атомарна
        """
        if not self._initialized:
            raise RuntimeError("Event Store not initialized")

        async with self.async_session_maker() as session:
            try:
                # Проверка версии - предотвращение дублей
                result = await session.execute(
                    text("""
                        SELECT MAX(version) FROM event_store 
                        WHERE deal_id = :deal_id
                    """),
                    {"deal_id": event.deal_id}
                )
                max_version = result.scalar() or 0
                
                if event.version != max_version + 1:
                    logger.warning(
                        f"Version conflict for deal {event.deal_id}: "
                        f"expected {max_version + 1}, got {event.version}"
                    )
                    return False
                
                # Вставить событие
                await session.execute(
                    text("""
                        INSERT INTO event_store 
                        (event_id, deal_id, event_type, event_data, actor, timestamp, version)
                        VALUES (:event_id, :deal_id, :event_type, :event_data, :actor, :timestamp, :version)
                    """),
                    {
                        "event_id": event.event_id,
                        "deal_id": event.deal_id,
                        "event_type": event.event_type,
                        "event_data": event.event_data,
                        "actor": event.actor,
                        "timestamp": event.timestamp,
                        "version": event.version
                    }
                )
                
                # Обновить проекцию (для быстрого чтения)
                await self._update_projection(session, event)
                
                # Создать снимок если необходимо
                if event.version % self.snapshot_interval == 0:
                    await self._create_snapshot(session, event.deal_id)
                
                await session.commit()
                logger.debug(f"Event appended: {event.event_type} for deal {event.deal_id} v{event.version}")
                return True
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Failed to append event: {str(e)}")
                raise

    async def get_events(
        self,
        deal_id: str,
        from_version: int = 0,
        to_version: Optional[int] = None
    ) -> List[DealEvent]:
        """Получить события по диапазону версий"""
        if not self._initialized:
            raise RuntimeError("Event Store not initialized")

        async with self.async_session_maker() as session:
            try:
                query = """
                    SELECT event_id, deal_id, event_type, event_data, actor, timestamp, version
                    FROM event_store
                    WHERE deal_id = :deal_id AND version > :from_version
                """
                params = {
                    "deal_id": deal_id,
                    "from_version": from_version
                }
                
                if to_version is not None:
                    query += " AND version <= :to_version"
                    params["to_version"] = to_version
                
                query += " ORDER BY version ASC"
                
                result = await session.execute(text(query), params)
                rows = result.fetchall()
                
                events = [
                    DealEvent(
                        event_id=row[0],
                        deal_id=row[1],
                        event_type=row[2],
                        event_data=row[3],
                        actor=row[4],
                        timestamp=row[5],
                        version=row[6]
                    )
                    for row in rows
                ]
                
                logger.debug(f"Retrieved {len(events)} events for deal {deal_id}")
                return events
                
            except Exception as e:
                logger.error(f"Failed to get events: {str(e)}")
                raise

    async def get_events_since(
        self,
        deal_id: str,
        timestamp: datetime
    ) -> List[DealEvent]:
        """Получить события после определённого времени"""
        if not self._initialized:
            raise RuntimeError("Event Store not initialized")

        async with self.async_session_maker() as session:
            try:
                result = await session.execute(
                    text("""
                        SELECT event_id, deal_id, event_type, event_data, actor, timestamp, version
                        FROM event_store
                        WHERE deal_id = :deal_id AND timestamp > :timestamp
                        ORDER BY timestamp ASC
                    """),
                    {
                        "deal_id": deal_id,
                        "timestamp": timestamp
                    }
                )
                rows = result.fetchall()
                
                events = [
                    DealEvent(
                        event_id=row[0],
                        deal_id=row[1],
                        event_type=row[2],
                        event_data=row[3],
                        actor=row[4],
                        timestamp=row[5],
                        version=row[6]
                    )
                    for row in rows
                ]
                
                return events
                
            except Exception as e:
                logger.error(f"Failed to get events since timestamp: {str(e)}")
                raise

    async def get_all_events(self, deal_id: str) -> List[DealEvent]:
        """Получить все события сделки"""
        return await self.get_events(deal_id, from_version=0)

    async def save_snapshot(self, snapshot: DealSnapshot) -> bool:
        """Сохранить снимок состояния"""
        if not self._initialized:
            raise RuntimeError("Event Store not initialized")

        async with self.async_session_maker() as session:
            try:
                # Удалить старый снимок для этой сделки
                await session.execute(
                    text("DELETE FROM deal_snapshots WHERE deal_id = :deal_id"),
                    {"deal_id": snapshot.deal_id}
                )
                
                # Добавить новый
                await session.execute(
                    text("""
                        INSERT INTO deal_snapshots 
                        (snapshot_id, deal_id, status, current_state, version)
                        VALUES (:snapshot_id, :deal_id, :status, :current_state, :version)
                    """),
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "deal_id": snapshot.deal_id,
                        "status": snapshot.status.value,
                        "current_state": snapshot.current_state,
                        "version": snapshot.version
                    }
                )
                
                await session.commit()
                logger.debug(f"Snapshot saved for deal {snapshot.deal_id} v{snapshot.version}")
                return True
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Failed to save snapshot: {str(e)}")
                raise

    async def get_latest_snapshot(self, deal_id: str) -> Optional[DealSnapshot]:
        """Получить последний снимок"""
        if not self._initialized:
            raise RuntimeError("Event Store not initialized")

        async with self.async_session_maker() as session:
            try:
                result = await session.execute(
                    text("""
                        SELECT snapshot_id, deal_id, status, current_state, version, created_at
                        FROM deal_snapshots
                        WHERE deal_id = :deal_id
                        ORDER BY version DESC
                        LIMIT 1
                    """),
                    {"deal_id": deal_id}
                )
                row = result.fetchone()
                
                if not row:
                    return None
                
                return DealSnapshot(
                    snapshot_id=row[0],
                    deal_id=row[1],
                    status=DealStatus(row[2]),
                    current_state=row[3],
                    version=row[4]
                )
                
            except Exception as e:
                logger.error(f"Failed to get latest snapshot: {str(e)}")
                raise

    async def rebuild_deal_state(
        self,
        deal_id: str,
        snapshot: Optional[DealSnapshot] = None
    ) -> Deal:
        """
        Восстановить состояние сделки через replay событий
        
        Стратегия:
        1. Если есть снимок, начать с его состояния
        2. Применить события после снимка в порядке
        3. Пересчитать все производные данные
        """
        if not self._initialized:
            raise RuntimeError("Event Store not initialized")

        try:
            # Получить снимок если не передан
            if snapshot is None:
                snapshot = await self.get_latest_snapshot(deal_id)
            
            # Начать с состояния снимка или пусто
            if snapshot:
                deal_state = snapshot.current_state
                from_version = snapshot.version
                logger.debug(f"Starting replay from snapshot v{snapshot.version} for deal {deal_id}")
            else:
                deal_state = {}
                from_version = 0
                logger.debug(f"Starting full replay for deal {deal_id}")
            
            # Получить события после снимка
            events = await self.get_events(deal_id, from_version)
            
            # Применить события к состоянию
            for event in events:
                deal_state = await self._apply_event(deal_state, event)
            
            # Создать объект Deal из состояния
            # В реальности здесь будет десериализация
            logger.debug(f"Rebuilt deal state for {deal_id} after {len(events)} events")
            return deal_state
            
        except Exception as e:
            logger.error(f"Failed to rebuild deal state: {str(e)}")
            raise

    async def _apply_event(self, state: Dict[str, Any], event: DealEvent) -> Dict[str, Any]:
        """Применить событие к состоянию"""
        if event.event_type == "STATUS_CHANGED":
            state["status"] = event.event_data.get("to")
        elif event.event_type == "PROPOSAL_GENERATED":
            state["proposal_id"] = event.event_data.get("proposal_id")
            state["estimated_value"] = event.event_data.get("estimated_value")
        elif event.event_type == "PAYMENT_COMPLETED":
            state["payment_status"] = "completed"
            state["actual_value"] = event.event_data.get("amount")
        
        return state

    async def _update_projection(self, session: AsyncSession, event: DealEvent):
        """Обновить read model проекцию для быстрого чтения"""
        try:
            # Проверить существует ли проекция
            result = await session.execute(
                text("SELECT deal_id FROM deal_projections WHERE deal_id = :deal_id"),
                {"deal_id": event.deal_id}
            )
            exists = result.scalar() is not None
            
            if exists:
                # Обновить
                await session.execute(
                    text("""
                        UPDATE deal_projections
                        SET status = :status, updated_at = CURRENT_TIMESTAMP
                        WHERE deal_id = :deal_id
                    """),
                    {
                        "deal_id": event.deal_id,
                        "status": event.event_data.get("to", event.event_data.get("status", ""))
                    }
                )
            else:
                # Создать
                await session.execute(
                    text("""
                        INSERT INTO deal_projections (deal_id, status, created_at, updated_at)
                        VALUES (:deal_id, :status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """),
                    {
                        "deal_id": event.deal_id,
                        "status": event.event_data.get("to", event.event_data.get("status", "lead"))
                    }
                )
        except Exception as e:
            logger.error(f"Failed to update projection: {str(e)}")
            # Не критично, продолжаем

    async def _create_snapshot(self, session: AsyncSession, deal_id: str):
        """Создать снимок состояния"""
        try:
            # Получить последний снимок
            result = await session.execute(
                text("SELECT version FROM deal_snapshots WHERE deal_id = :deal_id"),
                {"deal_id": deal_id}
            )
            last_snapshot_version = result.scalar() or 0
            
            # Получить все события после снимка
            events = await self.get_events(deal_id, from_version=last_snapshot_version)
            if not events:
                return
            
            # Пересчитать состояние
            state = {}
            for event in events:
                state = await self._apply_event(state, event)
            
            # Сохранить снимок
            snapshot = DealSnapshot(
                deal_id=deal_id,
                status=DealStatus(state.get("status", "lead")),
                current_state=state,
                version=events[-1].version
            )
            
            await session.execute(
                text("""
                    DELETE FROM deal_snapshots WHERE deal_id = :deal_id
                """),
                {"deal_id": deal_id}
            )
            
            await session.execute(
                text("""
                    INSERT INTO deal_snapshots 
                    (snapshot_id, deal_id, status, current_state, version)
                    VALUES (:snapshot_id, :deal_id, :status, :current_state, :version)
                """),
                {
                    "snapshot_id": snapshot.snapshot_id,
                    "deal_id": deal_id,
                    "status": state.get("status", "lead"),
                    "current_state": state,
                    "version": events[-1].version
                }
            )
            
            logger.debug(f"Snapshot created for deal {deal_id} v{events[-1].version}")
            
        except Exception as e:
            logger.error(f"Failed to create snapshot: {str(e)}")
            # Не критично

    async def close(self):
        """Закрыть подключение"""
        if self.engine:
            await self.engine.dispose()
            self._initialized = False
            logger.info("Event Store closed")


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

async def create_event_store(database_url: str) -> PostgreSQLEventStore:
    """Создать и инициализировать Event Store"""
    store = PostgreSQLEventStore(database_url)
    await store.initialize()
    return store
