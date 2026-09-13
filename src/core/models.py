"""
Core Domain Models for Autonomous Revenue Platform v2.0
Используются во всех модулях платформы
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum
import uuid
from pydantic import BaseModel, Field, validator


# ============================================================================
# ENUMS
# ============================================================================

class DealStatus(str, Enum):
    """Статусы сделки в жизненном цикле"""
    LEAD = "lead"
    QUALIFIED = "qualified"
    PROPOSAL_SENT = "proposal_sent"
    NEGOTIATION = "negotiation"
    WON = "won"
    LOST = "lost"
    STUCK = "stuck"


class RiskLevel(str, Enum):
    """Уровни риска"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class PaymentStatus(str, Enum):
    """Статусы платежа"""
    PENDING = "pending"
    PARTIAL = "partial"
    COMPLETED = "completed"
    OVERDUE = "overdue"


class NotificationChannel(str, Enum):
    """Каналы доставки уведомлений"""
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    SLACK = "slack"


# ============================================================================
# COMPANY & CONTACT MODELS
# ============================================================================

class ContactInfo(BaseModel):
    """Информация о контактном лице"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    email: str
    phone: Optional[str] = None
    title: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @validator('email')
    def validate_email(cls, v):
        if '@' not in v:
            raise ValueError('Invalid email format')
        return v.lower()


class CompanyData(BaseModel):
    """Информация о компании"""
    id: str
    name: str
    industry: Optional[str] = None
    website: Optional[str] = None
    revenue: Optional[float] = None
    employees: Optional[int] = None
    location: Optional[str] = None
    decision_makers: List[ContactInfo] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ============================================================================
# LEAD & QUALIFICATION MODELS
# ============================================================================

class LeadQualificationResult(BaseModel):
    """Результат квалификации лида"""
    lead_id: str
    is_qualified: bool
    score: float = Field(ge=0.0, le=1.0)
    reason: str
    risk_level: RiskLevel
    recommended_next_step: str
    scoring_details: Dict[str, float] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ============================================================================
# PROPOSAL MODELS
# ============================================================================

class ProposalLine(BaseModel):
    """Строка в коммерческом предложении"""
    description: str
    quantity: float
    unit_price: float
    total: float = Field(default=0.0)

    def calculate_total(self):
        self.total = self.quantity * self.unit_price
        return self.total


class ProposalGenerationResult(BaseModel):
    """Результат генерации коммерческого предложения"""
    proposal_id: str = Field(default_factory=lambda: f"PROP-{uuid.uuid4().hex[:8]}")
    deal_id: str
    content: str
    lines: List[ProposalLine] = Field(default_factory=list)
    estimated_value: float = Field(ge=0.0)
    validity_days: int = Field(default=30, ge=1)
    valid_until: datetime = Field(default_factory=lambda: datetime.utcnow() + timedelta(days=30))
    generated_by: str = Field(default="AI_GENERATION_ENGINE")
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @validator('lines', pre=True, always=True)
    def calculate_total_value(cls, v, values):
        if 'estimated_value' in values and not v:
            return v
        total = sum(line.total if isinstance(line, ProposalLine) else line.get('total', 0) for line in v)
        if total > 0:
            values['estimated_value'] = total
        return v


# ============================================================================
# PAYMENT MODELS
# ============================================================================

class PaymentCheckResult(BaseModel):
    """Результат проверки статуса платежа"""
    payment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deal_id: str
    payment_status: PaymentStatus
    amount_due: float = Field(ge=0.0)
    amount_paid: float = Field(ge=0.0, default=0.0)
    days_overdue: int = Field(ge=0, default=0)
    risk_score: float = Field(ge=0.0, le=1.0)
    next_check_at: datetime = Field(default_factory=lambda: datetime.utcnow() + timedelta(hours=24))
    checked_at: datetime = Field(default_factory=datetime.utcnow)


# ============================================================================
# EVENT & AUDIT MODELS
# ============================================================================

class DealEvent(BaseModel):
    """Событие в истории сделки (Event Sourcing)"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deal_id: str
    event_type: str
    event_data: Dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(default="SYSTEM")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: int = Field(default=1)

    class Config:
        frozen = True  # Immutable для Event Sourcing


class DealSnapshot(BaseModel):
    """Снимок состояния сделки для оптимизации (Event Sourcing)"""
    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deal_id: str
    status: DealStatus
    current_state: Dict[str, Any]
    version: int
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ============================================================================
# SAGA COMPENSATION MODEL
# ============================================================================

class DealSagaCompensation(BaseModel):
    """Компенсирующая транзакция для Saga pattern"""
    compensation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deal_id: str
    action: str  # "rollback_1c", "rollback_payment", "revert_proposal"
    entity_id: str
    external_system: str = Field(default="1C")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    reason: str
    retry_count: int = Field(default=0, ge=0)
    max_retries: int = Field(default=3)
    is_completed: bool = Field(default=False)


# ============================================================================
# DEAL AGGREGATE ROOT
# ============================================================================

class Deal(BaseModel):
    """
    Deal aggregate root - управляет жизненным циклом сделки
    Используется в Event Sourcing + CQRS паттернах
    """
    deal_id: str = Field(default_factory=lambda: f"DEAL-{uuid.uuid4().hex[:8]}")
    status: DealStatus = Field(default=DealStatus.LEAD)
    company: CompanyData
    primary_contact: ContactInfo
    requirements: str
    proposal_id: Optional[str] = None
    estimated_value: float = Field(ge=0.0, default=0.0)
    actual_value: float = Field(ge=0.0, default=0.0)
    
    # Temporal workflow tracking
    workflow_run_id: Optional[str] = None
    
    # Event sourcing
    events: List[DealEvent] = Field(default_factory=list)
    version: int = Field(default=0)
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    qualified_at: Optional[datetime] = None
    proposal_sent_at: Optional[datetime] = None
    won_at: Optional[datetime] = None
    lost_at: Optional[datetime] = None
    
    # Metadata
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def add_event(self, event_type: str, event_data: Dict[str, Any], actor: str = "SYSTEM") -> DealEvent:
        """Добавить событие к сделке (Event Sourcing)"""
        event = DealEvent(
            deal_id=self.deal_id,
            event_type=event_type,
            event_data=event_data,
            actor=actor,
            version=self.version + 1
        )
        self.events.append(event)
        self.version += 1
        return event

    def transition_to(self, new_status: DealStatus, reason: str = ""):
        """Безопасный переход сделки в новый статус"""
        valid_transitions = {
            DealStatus.LEAD: [DealStatus.QUALIFIED, DealStatus.LOST],
            DealStatus.QUALIFIED: [DealStatus.PROPOSAL_SENT, DealStatus.LOST],
            DealStatus.PROPOSAL_SENT: [DealStatus.NEGOTIATION, DealStatus.STUCK, DealStatus.LOST],
            DealStatus.NEGOTIATION: [DealStatus.WON, DealStatus.LOST, DealStatus.STUCK],
            DealStatus.STUCK: [DealStatus.NEGOTIATION, DealStatus.LOST],
            DealStatus.WON: [],
            DealStatus.LOST: [],
        }

        if new_status not in valid_transitions.get(self.status, []):
            raise ValueError(
                f"Cannot transition from {self.status} to {new_status}. "
                f"Valid transitions: {valid_transitions.get(self.status, [])}"
            )

        old_status = self.status
        self.status = new_status
        
        # Добавить событие о переходе
        self.add_event(
            "STATUS_CHANGED",
            {
                "from": old_status,
                "to": new_status,
                "reason": reason
            }
        )

        # Установить timestamp для ключевых переходов
        now = datetime.utcnow()
        if new_status == DealStatus.QUALIFIED:
            self.qualified_at = now
        elif new_status == DealStatus.PROPOSAL_SENT:
            self.proposal_sent_at = now
        elif new_status == DealStatus.WON:
            self.won_at = now
        elif new_status == DealStatus.LOST:
            self.lost_at = now


# ============================================================================
# NOTIFICATION MODELS
# ============================================================================

class Notification(BaseModel):
    """Уведомление для отправки"""
    notification_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deal_id: str
    recipient: str
    notification_type: str
    message: str
    channel: NotificationChannel = NotificationChannel.EMAIL
    metadata: Dict[str, Any] = Field(default_factory=dict)
    scheduled_at: datetime = Field(default_factory=datetime.utcnow)
    sent_at: Optional[datetime] = None
    is_sent: bool = Field(default=False)


# ============================================================================
# AI COACH RECOMMENDATION
# ============================================================================

class AiCoachRecommendation(BaseModel):
    """Рекомендация от AI coach"""
    recommendation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deal_id: str
    recommendation: str
    urgency: str = Field(default="medium")  # low, medium, high, critical
    reasoning: str
    suggested_action: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
