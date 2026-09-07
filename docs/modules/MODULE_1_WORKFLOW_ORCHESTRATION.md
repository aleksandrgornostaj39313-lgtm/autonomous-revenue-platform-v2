# MODULE 1: DISTRIBUTED WORKFLOW ORCHESTRATION
## Полная реализация на Temporal

```python
# src/orchestration/workflow_engine.py

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Dict, Any, List
from enum import Enum
import json
import logging

from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.runtime import Runtime
from temporalio.common import RetryPolicy
import asyncio
from pydantic import BaseModel, Field

# ============================================================================
# MODELS & TYPES
# ============================================================================

class DealStatus(str, Enum):
    LEAD = "lead"
    QUALIFIED = "qualified"
    PROPOSAL_SENT = "proposal_sent"
    NEGOTIATION = "negotiation"
    WON = "won"
    LOST = "lost"
    STUCK = "stuck"


class LeadQualificationResult(BaseModel):
    is_qualified: bool
    score: float  # 0-1
    reason: str
    risk_level: str  # low, medium, high
    recommended_next_step: str


class ProposalGenerationResult(BaseModel):
    proposal_id: str
    content: str
    estimated_value: float
    validity_days: int
    generated_at: str


class PaymentCheckResult(BaseModel):
    payment_status: str  # pending, partial, completed
    amount_due: float
    days_overdue: int
    risk_score: float


class DealSagaCompensation(BaseModel):
    action: str  # "rollback_1c", "rollback_payment", "revert_proposal"
    entity_id: str
    timestamp: str
    reason: str


# ============================================================================
# ACTIVITIES (External integrations)
# ============================================================================

@activity.defn
async def qualify_lead(lead_id: str, company_data: Dict[str, Any]) -> LeadQualificationResult:
    """
    Активность: квалификация лида через AI scoring engine
    Интегрирует с core/lead_filter.py
    """
    logging.info(f"Qualifying lead {lead_id} from company {company_data.get('name')}")
    
    # Имитация вызова AI scoring engine
    # В реальности это будет вызов к services/assistant.py
    score = 0.75 if company_data.get('revenue', 0) > 1000000 else 0.45
    risk = "low" if score > 0.7 else "medium"
    
    return LeadQualificationResult(
        is_qualified=score > 0.6,
        score=score,
        reason="High revenue company with matching pain points",
        risk_level=risk,
        recommended_next_step="Send personalized proposal"
    )


@activity.defn
async def generate_proposal(
    lead_id: str,
    company_name: str,
    contact_name: str,
    requirements: str
) -> ProposalGenerationResult:
    """
    Активность: генерация КП через AI
    Интегрирует с services/constructor.py и AI/LLM layer
    """
    logging.info(f"Generating proposal for {company_name}")
    
    # В реальности вызывает LLM для генерации
    proposal_content = f"""
    Коммерческое предложение

    Для: {company_name}
    Контакт: {contact_name}
    Дата: {asyncio.get_event_loop().time()}

    На основе Ваших требований:
    {requirements}

    Предлагаем решение:
    - Консультация по внедрению 1С: 150,000 руб.
    - Кастомизация под процессы: 200,000 руб.
    - Обучение команды: 50,000 руб.
    
    Итого: 400,000 руб.
    Действительно 30 дней.
    """
    
    return ProposalGenerationResult(
        proposal_id=f"PROP-{lead_id}-001",
        content=proposal_content,
        estimated_value=400000.0,
        validity_days=30,
        generated_at="2024-01-15T10:30:00Z"
    )


@activity.defn
async def send_proposal_to_1c(
    lead_id: str,
    proposal_id: str,
    company_id: str,
    estimated_value: float
) -> Dict[str, Any]:
    """
    Активность: отправка КП в 1С
    Интегрирует с integrations/1c/http_api_connector.py
    """
    logging.info(f"Sending proposal {proposal_id} to 1C for company {company_id}")
    
    # В реальности вызывает 1C REST API через OData или HTTP API
    # Результат содержит ID документа в 1С
    return {
        "success": True,
        "1c_document_id": f"DOC-{company_id}-2024-001",
        "1c_doc_number": "КП-0001/2024",
        "sync_timestamp": "2024-01-15T10:35:00Z"
    }


@activity.defn
async def check_payment_status(
    deal_id: str,
    expected_amount: float
) -> PaymentCheckResult:
    """
    Активность: проверка статуса платежа
    Интегрирует с integrations/payments/payment_gateway.py и risk_engine.py
    """
    logging.info(f"Checking payment status for deal {deal_id}, amount {expected_amount}")
    
    # В реальности запрашивает платежную систему + 1С
    # Включает проверку просрочек через payment_risk_engine.py
    return PaymentCheckResult(
        payment_status="partial",
        amount_due=100000.0,
        days_overdue=5,
        risk_score=0.35
    )


@activity.defn
async def send_notification(
    recipient: str,
    notification_type: str,
    message: str,
    channel: str = "email"
) -> Dict[str, Any]:
    """
    Активность: отправка уведомления
    Интегрирует с notifications/notifier.py
    """
    logging.info(f"Sending {notification_type} notification to {recipient} via {channel}")
    
    return {
        "success": True,
        "notification_id": f"NOTIF-{recipient}-{notification_type}",
        "sent_at": "2024-01-15T10:40:00Z",
        "channel": channel
    }


@activity.defn
async def trigger_ai_coach(
    deal_id: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Активность: вызов AI coach для рекомендаций
    Интегрирует с services/coach.py и AI/LLM layer
    """
    logging.info(f"Triggering AI coach for deal {deal_id}")
    
    # Вызывает LLM agent для анализа ситуации и рекомендаций
    return {
        "recommendation": "Follow up with technical requirements gathering",
        "urgency": "high",
        "reasoning": "Deal stuck for 3 days without response",
        "suggested_action": "Call decision maker directly"
    }


@activity.defn
async def compensate_1c_operation(compensation: DealSagaCompensation) -> bool:
    """
    Активность: компенсирующая транзакция (откат в 1С)
    Для Saga pattern при ошибках
    """
    logging.warning(f"Compensating 1C operation: {compensation.action} for {compensation.entity_id}")
    logging.info(f"Reason: {compensation.reason}")
    
    # Откатывает операцию в 1С (удаление документа, реверс и т.д.)
    return True


# ============================================================================
# WORKFLOW (Orchestration logic)
# ============================================================================

@workflow.defn
class DealLifecycleWorkflow:
    """
    Основной workflow для управления жизненным циклом сделки
    Lead → Qualified → Proposal → Negotiation → Closed
    """
    
    def __init__(self):
        self.current_status = DealStatus.LEAD
        self.events: List[Dict[str, Any]] = []
        self.compensations: List[DealSagaCompensation] = []
    
    @workflow.run
    async def execute(
        self,
        deal_id: str,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str
    ) -> Dict[str, Any]:
        """
        Основной workflow: обработка сделки от лида до закрытия
        """
        
        logging.info(f"Starting deal lifecycle workflow for deal {deal_id}")
        
        # Retries: exponential backoff (1s, 2s, 4s, 8s)
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=8),
            maximum_attempts=4
        )
        
        try:
            # ===== STEP 1: Qualify Lead =====
            self._log_event("QUALIFY_LEAD_START", {"deal_id": deal_id})
            
            qual_result = await workflow.execute_activity(
                qualify_lead,
                lead_id=deal_id,
                company_data=company_data,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry_policy
            )
            
            self._log_event("QUALIFY_LEAD_COMPLETE", {
                "is_qualified": qual_result.is_qualified,
                "score": qual_result.score
            })
            
            if not qual_result.is_qualified:
                self.current_status = DealStatus.LOST
                self._log_event("DEAL_REJECTED", {"reason": qual_result.reason})
                return {"status": "rejected", "reason": qual_result.reason}
            
            self.current_status = DealStatus.QUALIFIED
            
            # ===== STEP 2: Generate Proposal =====
            self._log_event("PROPOSAL_GENERATION_START", {"deal_id": deal_id})
            
            proposal = await workflow.execute_activity(
                generate_proposal,
                lead_id=deal_id,
                company_name=company_data.get("name"),
                contact_name=contact_info.get("name"),
                requirements=requirements,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy
            )
            
            self._log_event("PROPOSAL_GENERATED", {
                "proposal_id": proposal.proposal_id,
                "value": proposal.estimated_value
            })
            
            # ===== STEP 3: Saga - Send to 1C =====
            self._log_event("1C_SYNC_START", {"proposal_id": proposal.proposal_id})
            
            try:
                sync_result = await workflow.execute_activity(
                    send_proposal_to_1c,
                    lead_id=deal_id,
                    proposal_id=proposal.proposal_id,
                    company_id=company_data.get("id"),
                    estimated_value=proposal.estimated_value,
                    start_to_close_timeout=timedelta(seconds=45),
                    retry_policy=retry_policy
                )
                
                self._log_event("1C_SYNC_SUCCESS", sync_result)
                
                # Сохраняем компенсацию для отката
                self.compensations.append(DealSagaCompensation(
                    action="rollback_1c",
                    entity_id=sync_result.get("1c_document_id"),
                    timestamp="2024-01-15T10:35:00Z",
                    reason="Saga compensation point"
                ))
                
            except Exception as e:
                logging.error(f"Failed to sync with 1C: {str(e)}")
                self._log_event("1C_SYNC_FAILED", {"error": str(e)})
                # Откатываем
                await self._compensate_all()
                raise
            
            self.current_status = DealStatus.PROPOSAL_SENT
            
            # ===== STEP 4: Wait for Response (with timeout) =====
            self._log_event("WAITING_FOR_RESPONSE", {"proposal_id": proposal.proposal_id})
            
            # Ждём 14 дней на ответ
            response_timeout = timedelta(days=14)
            
            # Используем signal для получения ответа от внешних систем
            response_received = False
            try:
                await workflow.wait_condition(
                    lambda: self._has_response(),
                    timeout=response_timeout
                )
                response_received = True
                self._log_event("RESPONSE_RECEIVED", {})
            except asyncio.TimeoutError:
                self._log_event("RESPONSE_TIMEOUT", {"days": 14})
                # Отправляем напоминание
                await workflow.execute_activity(
                    send_notification,
                    recipient=contact_info.get("email"),
                    notification_type="follow_up",
                    message="Reminder: Your proposal is expiring soon",
                    channel="email",
                    start_to_close_timeout=timedelta(seconds=30)
                )
            
            if not response_received:
                self.current_status = DealStatus.STUCK
                self._log_event("DEAL_STUCK", {"reason": "No response after 14 days"})
                
                # Вызываем AI coach для рекомендаций
                coach_result = await workflow.execute_activity(
                    trigger_ai_coach,
                    deal_id=deal_id,
                    context={"status": "stuck", "days_without_response": 14},
                    start_to_close_timeout=timedelta(seconds=30)
                )
                
                self._log_event("AI_COACH_RECOMMENDATION", coach_result)
            
            # ===== STEP 5: Payment Tracking =====
            self._log_event("PAYMENT_TRACKING_START", {"deal_id": deal_id})
            
            payment_result = await workflow.execute_activity(
                check_payment_status,
                deal_id=deal_id,
                expected_amount=proposal.estimated_value,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry_policy
            )
            
            self._log_event("PAYMENT_STATUS_CHECKED", {
                "status": payment_result.payment_status,
                "overdue_days": payment_result.days_overdue,
                "risk_score": payment_result.risk_score
            })
            
            if payment_result.days_overdue > 0:
                # Отправляем уведомление о просрочке
                await workflow.execute_activity(
                    send_notification,
                    recipient="finance@company.com",
                    notification_type="payment_overdue",
                    message=f"Payment overdue by {payment_result.days_overdue} days",
                    channel="email",
                    start_to_close_timeout=timedelta(seconds=30)
                )
            
            # ===== FINAL STATUS =====
            if payment_result.payment_status == "completed":
                self.current_status = DealStatus.WON
                self._log_event("DEAL_WON", {"deal_id": deal_id})
            
            return {
                "deal_id": deal_id,
                "final_status": self.current_status,
                "proposal_id": proposal.proposal_id,
                "estimated_value": proposal.estimated_value,
                "events": self.events
            }
            
        except Exception as e:
            logging.error(f"Deal workflow failed: {str(e)}")
            self.current_status = DealStatus.LOST
            await self._compensate_all()
            raise
    
    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Логирование события в audit trail"""
        event = {
            "type": event_type,
            "timestamp": workflow.now().isoformat(),
            "data": data
        }
        self.events.append(event)
        logging.info(f"Event: {event_type} - {data}")
    
    def _has_response(self) -> bool:
        """Проверка получен ли ответ (через signal)"""
        # Переопределяется через @workflow.signal
        return getattr(self, '_response_received', False)
    
    @workflow.signal
    def mark_response_received(self):
        """Signal от внешних систем (Bitrix, email и т.д.)"""
        self._response_received = True
        self._log_event("SIGNAL_RESPONSE_RECEIVED", {})
    
    async def _compensate_all(self):
        """Компенсирующие транзакции для Saga pattern"""
        logging.warning(f"Starting saga compensation with {len(self.compensations)} steps")
        
        for compensation in reversed(self.compensations):
            try:
                await workflow.execute_activity(
                    compensate_1c_operation,
                    compensation=compensation,
                    start_to_close_timeout=timedelta(seconds=30)
                )
                self._log_event("COMPENSATION_SUCCESS", {
                    "action": compensation.action,
                    "entity_id": compensation.entity_id
                })
            except Exception as e:
                logging.error(f"Compensation failed: {str(e)}")
                self._log_event("COMPENSATION_FAILED", {
                    "action": compensation.action,
                    "error": str(e)
                })


# ============================================================================
# CLIENT (Workflow invocation)
# ============================================================================

class WorkflowOrchestrator:
    """
    Client для запуска и управления workflows
    """
    
    def __init__(self, temporal_server_host: str = "localhost:7233"):
        self.server_host = temporal_server_host
        self.client = None
    
    async def connect(self):
        """Подключение к Temporal сервису"""
        self.client = await Client.connect(self.server_host)
    
    async def start_deal_workflow(
        self,
        deal_id: str,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str
    ) -> str:
        """
        Запуск workflow для обработки сделки
        
        Args:
            deal_id: Уникальный ID сделки
            company_data: {"id": "...", "name": "...", "revenue": "..."}
            contact_info: {"name": "...", "email": "...", "phone": "..."}
            requirements: Требования от компании
        
        Returns:
            workflow_run_id: ID для отслеживания выполнения
        """
        if not self.client:
            await self.connect()
        
        workflow_run_id = f"deal-{deal_id}-{int(asyncio.get_event_loop().time())}"
        
        await self.client.execute_workflow(
            DealLifecycleWorkflow.execute,
            deal_id=deal_id,
            company_data=company_data,
            contact_info=contact_info,
            requirements=requirements,
            id=workflow_run_id,
            task_queue="deal_processing"
        )
        
        logging.info(f"Started workflow {workflow_run_id} for deal {deal_id}")
        return workflow_run_id
    
    async def get_workflow_status(self, workflow_run_id: str) -> Dict[str, Any]:
        """Получить статус выполнения workflow"""
        if not self.client:
            await self.connect()
        
        handle = self.client.get_workflow_handle(workflow_run_id)
        desc = await handle.describe()
        
        return {
            "workflow_id": desc.workflow_id,
            "run_id": desc.run_id,
            "status": desc.status.name,
            "start_time": desc.start_time.isoformat(),
            "close_time": desc.close_time.isoformat() if desc.close_time else None
        }
    
    async def signal_response_received(self, workflow_run_id: str) -> bool:
        """Отправить signal что ответ получен"""
        if not self.client:
            await self.connect()
        
        handle = self.client.get_workflow_handle(workflow_run_id)
        await handle.signal(DealLifecycleWorkflow.mark_response_received)
        
        logging.info(f"Signaled response_received to {workflow_run_id}")
        return True
    
    async def close(self):
        """Закрытие соединения"""
        if self.client:
            await self.client.aclose()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

async def main():
    """Пример использования workflow orchestrator"""
    
    orchestrator = WorkflowOrchestrator()
    await orchestrator.connect()
    
    # Запуск workflow
    workflow_id = await orchestrator.start_deal_workflow(
        deal_id="DEAL-001",
        company_data={
            "id": "COMP-001",
            "name": "ООО Рога и Копыта",
            "revenue": 50000000
        },
        contact_info={
            "name": "Иван Петров",
            "email": "ivan@company.ru",
            "phone": "+7-900-123-4567"
        },
        requirements="Нужна система управления учетом для 1С"
    )
    
    # Проверка статуса
    status = await orchestrator.get_workflow_status(workflow_id)
    print(f"Workflow status: {status}")
    
    # Отправка signal после ответа компании
    await asyncio.sleep(5)
    await orchestrator.signal_response_received(workflow_id)
    
    await orchestrator.close()


if __name__ == "__main__":
    asyncio.run(main())
```

---

## src/orchestration/state_machine.py

```python
# Deal FSM (Finite State Machine)

from enum import Enum
from typing import Optional, Callable, Dict, Any
from datetime import datetime
import logging

class DealTransition:
    """Переход между состояниями"""
    
    def __init__(
        self,
        from_state: str,
        to_state: str,
        condition: Optional[Callable[..., bool]] = None,
        on_transition: Optional[Callable[..., None]] = None
    ):
        self.from_state = from_state
        self.to_state = to_state
        self.condition = condition or (lambda *args, **kwargs: True)
        self.on_transition = on_transition


class DealStateMachine:
    """
    Deal lifecycle state machine
    
    Состояния:
    lead → qualified → proposal_sent → negotiation → won/lost/stuck
    """
    
    def __init__(self, deal_id: str):
        self.deal_id = deal_id
        self.current_state = "lead"
        self.transitions: Dict[str, list] = {
            "lead": [DealTransition("lead", "qualified")],
            "qualified": [DealTransition("qualified", "proposal_sent")],
            "proposal_sent": [
                DealTransition("proposal_sent", "negotiation"),
                DealTransition("proposal_sent", "stuck")
            ],
            "negotiation": [
                DealTransition("negotiation", "won"),
                DealTransition("negotiation", "lost"),
                DealTransition("negotiation", "stuck")
            ],
            "won": [],
            "lost": [],
            "stuck": [DealTransition("stuck", "negotiation")]
        }
        self.history: list = []
    
    def can_transition(self, to_state: str, **context) -> bool:
        """Проверить возможность перехода"""
        transitions = self.transitions.get(self.current_state, [])
        
        for trans in transitions:
            if trans.to_state == to_state and trans.condition(**context):
                return True
        
        return False
    
    def transition(self, to_state: str, reason: str = "", **context) -> bool:
        """Выполнить переход"""
        if not self.can_transition(to_state, **context):
            logging.warning(f"Cannot transition from {self.current_state} to {to_state}")
            return False
        
        from_state = self.current_state
        self.current_state = to_state
        
        self.history.append({
            "timestamp": datetime.utcnow().isoformat(),
            "from": from_state,
            "to": to_state,
            "reason": reason
        })
        
        logging.info(f"Deal {self.deal_id} transitioned: {from_state} → {to_state}")
        return True
    
    def get_history(self) -> list:
        """Получить историю переходов"""
        return self.history
```

Продолжение в следующих файлах...
