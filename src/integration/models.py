"""
MODULE 8: Enterprise Integration Hub - Domain Models
Production-grade data models for 1C, Bitrix24, and payment gateway integrations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class IntegrationSource(str, Enum):
    """Supported external systems"""
    ONE_C = "1c"
    BITRIX24 = "bitrix24"
    HUBSPOT = "hubspot"
    SALESFORCE = "salesforce"
    YANDEX_KASSA = "yandex_kassa"
    STRIPE = "stripe"
    PAYPAL = "paypal"


class SyncDirection(str, Enum):
    """Bidirectional sync direction"""
    PULL = "pull"  # From external to platform
    PUSH = "push"  # From platform to external
    BIDIRECTIONAL = "bidirectional"


class ConflictResolutionStrategy(str, Enum):
    """How to resolve conflicts between systems"""
    LAST_WRITE_WINS = "last_write_wins"
    MANUAL_REVIEW = "manual_review"
    PLATFORM_AUTHORITY = "platform_authority"
    EXTERNAL_AUTHORITY = "external_authority"


class WebhookEventType(str, Enum):
    """Types of events from external systems"""
    DEAL_CREATED = "deal_created"
    DEAL_UPDATED = "deal_updated"
    DEAL_CLOSED = "deal_closed"
    PAYMENT_RECEIVED = "payment_received"
    PAYMENT_FAILED = "payment_failed"
    CONTACT_CREATED = "contact_created"
    CONTACT_UPDATED = "contact_updated"
    DOCUMENT_CREATED = "document_created"
    DOCUMENT_STATUS_CHANGED = "document_status_changed"


class IntegrationStatus(str, Enum):
    """Status of integration connection"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    RETRYING = "retrying"
    PAUSED = "paused"


@dataclass
class IntegrationConfig:
    """Configuration for external system integration"""
    config_id: str
    source: IntegrationSource
    tenant_id: str  # Customer or organization ID
    api_endpoint: str
    api_key: str  # Encrypted in production
    api_secret: Optional[str] = None  # Encrypted in production
    sync_direction: SyncDirection = SyncDirection.BIDIRECTIONAL
    conflict_strategy: ConflictResolutionStrategy = ConflictResolutionStrategy.LAST_WRITE_WINS
    batch_size: int = 100
    retry_attempts: int = 3
    timeout_seconds: int = 30
    status: IntegrationStatus = IntegrationStatus.ACTIVE
    last_sync_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExternalEntityMapping:
    """Mapping between platform entity and external system entity"""
    mapping_id: str
    source: IntegrationSource
    platform_entity_type: str  # "deal", "contact", "company"
    external_entity_type: str  # e.g., "Opportunity" in Salesforce, "Deal" in Bitrix24
    platform_entity_id: str
    external_entity_id: str
    external_url: Optional[str] = None
    mapped_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SyncJobLog:
    """Log entry for sync operations"""
    job_id: str
    config_id: str
    source: IntegrationSource
    sync_type: str  # "full", "incremental"
    direction: SyncDirection
    started_at: datetime
    completed_at: Optional[datetime] = None
    total_records: int = 0
    synced_records: int = 0
    failed_records: int = 0
    error_message: Optional[str] = None
    status: str = "running"  # "running", "completed", "failed", "partial"
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SyncConflict:
    """Detected conflict during sync"""
    conflict_id: str
    mapping_id: str
    source: IntegrationSource
    conflict_type: str  # "value_mismatch", "timestamp_mismatch", "state_mismatch"
    platform_value: Any
    external_value: Any
    platform_timestamp: datetime
    external_timestamp: datetime
    resolution: Optional[str] = None  # "manual_review", "auto_resolved"
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class WebhookEvent:
    """Incoming webhook event from external system"""
    event_id: str
    source: IntegrationSource
    event_type: WebhookEventType
    external_entity_id: str
    external_entity_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str] = field(default_factory=dict)
    signature: Optional[str] = None  # For webhook verification
    received_at: datetime = field(default_factory=datetime.utcnow)
    processed: bool = False
    processed_at: Optional[datetime] = None
    error_message: Optional[str] = None


@dataclass
class PaymentGatewayConfig:
    """Configuration for payment gateway"""
    gateway_id: str
    provider: str  # "yandex_kassa", "stripe", "paypal"
    api_key: str  # Encrypted
    api_secret: Optional[str] = None  # Encrypted
    webhook_url: str
    return_url: str
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DataMapper:
    """Mapping configuration between platform and external field schemas"""
    mapper_id: str
    source: IntegrationSource
    platform_entity_type: str
    field_mappings: Dict[str, str]  # {"platform_field": "external_field"}
    transformations: Dict[str, Any] = field(default_factory=dict)  # Custom value transformations
    default_values: Dict[str, Any] = field(default_factory=dict)
    required_fields: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class IntegrationError:
    """Error during integration operation"""
    error_id: str
    config_id: str
    source: IntegrationSource
    error_type: str  # "connection_error", "data_validation_error", "rate_limit"
    error_message: str
    external_request_id: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: Optional[datetime] = None
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolution_notes: Optional[str] = None
