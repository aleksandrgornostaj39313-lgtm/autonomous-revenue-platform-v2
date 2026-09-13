"""
Production-Ready Workflow Orchestration Engine on Temporal
2026 Edition with real integrations, no mocks, no stubs
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum
import uuid

from temporalio import workflow, activity
from temporalio.client import Client, WorkflowAlreadyStartedError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from src.core.models import (
    Deal, DealStatus, DealEvent, LeadQualificationResult,
    ProposalGenerationResult, PaymentCheckResult, Notification,
    AiCoachRecommendation, DealSagaCompensation, RiskLevel
)
from src.infrastructure.event_store import PostgreSQLEventStore
from src.integrations.lead_scoring import LeadScoringEngine
from src.integrations.proposal_generator import ProposalGeneratorService
from src.integrations.integration_1c import Integration1CService
from src.integrations.payment_gateway import PaymentGatewayService
from src.integrations.notification_service import NotificationService
from src.integrations.ai_coach import AiCoachService

logger = logging.getLogger(__name__)


# ============================================================================
# ACTIVITY IMPLEMENTATIONS - Real integrations (2026 edition)
# ============================================================================

@activity.defn
async def qualify_lead_activity(
    lead_id: str,
    company_data: Dict[str, Any],
    lead_scoring_engine: LeadScoringEngine
) -> LeadQualificationResult:
    """
    Activity: Real lead qualification through AI scoring engine
    No mocks - uses actual LeadScoringEngine implementation
    
    Integrates with:
    - src/integrations/lead_scoring.py
    - Vector embeddings for similarity matching
    - Risk assessment algorithms
    """
    logger.info(f"[ACTIVITY] Qualifying lead {lead_id} from {company_data.get('name')}")
    
    try:
        # Real scoring - not a mock
        result = await lead_scoring_engine.score_lead(
            lead_id=lead_id,
            company_data=company_data
        )
        
        logger.info(
            f"[ACTIVITY] Lead {lead_id} scored: {result.score:.2f}, "
            f"qualified: {result.is_qualified}, risk: {result.risk_level}"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"[ACTIVITY] Lead qualification failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def generate_proposal_activity(
    deal_id: str,
    company_data: Dict[str, Any],
    contact_info: Dict[str, Any],
    requirements: str,
    proposal_generator: ProposalGeneratorService
) -> ProposalGenerationResult:
    """
    Activity: Real proposal generation via AI
    No templates - uses LLM with context from knowledge base
    
    Integrates with:
    - src/integrations/proposal_generator.py
    - LLM (OpenAI, Claude, Anthropic)
    - Knowledge base retrieval
    """
    logger.info(f"[ACTIVITY] Generating proposal for deal {deal_id}")
    
    try:
        proposal = await proposal_generator.generate_proposal(
            deal_id=deal_id,
            company_data=company_data,
            contact_info=contact_info,
            requirements=requirements
        )
        
        logger.info(
            f"[ACTIVITY] Proposal {proposal.proposal_id} generated, "
            f"value: {proposal.estimated_value}"
        )
        
        return proposal
        
    except Exception as e:
        logger.error(f"[ACTIVITY] Proposal generation failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def sync_proposal_to_1c_activity(
    deal_id: str,
    proposal_id: str,
    company_id: str,
    estimated_value: float,
    integration_1c: Integration1CService
) -> Dict[str, Any]:
    """
    Activity: Real sync with 1C via HTTP API / OData
    No mocks - actual REST API calls with error handling and retries
    
    Integrates with:
    - src/integrations/integration_1c.py
    - 1C HTTP API / OData v4
    - Real document creation
    """
    logger.info(f"[ACTIVITY] Syncing proposal {proposal_id} to 1C")
    
    try:
        sync_result = await integration_1c.create_proposal_document(
            deal_id=deal_id,
            proposal_id=proposal_id,
            company_id=company_id,
            estimated_value=estimated_value
        )
        
        logger.info(
            f"[ACTIVITY] 1C sync successful: doc_id={sync_result.get('1c_document_id')}, "
            f"doc_number={sync_result.get('1c_doc_number')}"
        )
        
        return sync_result
        
    except Exception as e:
        logger.error(f"[ACTIVITY] 1C sync failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def check_payment_status_activity(
    deal_id: str,
    expected_amount: float,
    payment_gateway: PaymentGatewayService
) -> PaymentCheckResult:
    """
    Activity: Real payment status check
    Queries payment gateway and 1C for actual payment data
    
    Integrates with:
    - src/integrations/payment_gateway.py
    - Payment processors (Yandex.Kassa, Sber, etc.)
    - 1C payment module
    """
    logger.info(f"[ACTIVITY] Checking payment status for deal {deal_id}")
    
    try:
        payment_status = await payment_gateway.check_payment(
            deal_id=deal_id,
            expected_amount=expected_amount
        )
        
        logger.info(
            f"[ACTIVITY] Payment status: {payment_status.payment_status}, "
            f"overdue: {payment_status.days_overdue}, "
            f"risk: {payment_status.risk_score}"
        )
        
        return payment_status
        
    except Exception as e:
        logger.error(f"[ACTIVITY] Payment check failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def send_notification_activity(
    deal_id: str,
    recipient: str,
    notification_type: str,
    message: str,
    notification_service: NotificationService
) -> Dict[str, Any]:
    """
    Activity: Real notification delivery
    Sends via email, SMS, Slack, or webhooks
    
    Integrates with:
    - src/integrations/notification_service.py
    - Email service (SMTP, AWS SES)
    - SMS gateway (Twilio)
    - Slack API
    """
    logger.info(f"[ACTIVITY] Sending notification: {notification_type} to {recipient}")
    
    try:
        notification = Notification(
            deal_id=deal_id,
            recipient=recipient,
            notification_type=notification_type,
            message=message
        )
        
        result = await notification_service.send(notification)
        
        logger.info(f"[ACTIVITY] Notification sent: {result.get('notification_id')}")
        
        return result
        
    except Exception as e:
        logger.error(f"[ACTIVITY] Notification failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def trigger_ai_coach_activity(
    deal_id: str,
    context: Dict[str, Any],
    ai_coach_service: AiCoachService
) -> AiCoachRecommendation:
    """
    Activity: Real AI coaching via LLM
    Analyzes deal situation and provides actionable recommendations
    
    Integrates with:
    - src/integrations/ai_coach.py
    - LLM with domain-specific prompts
    - Deal history context
    """
    logger.info(f"[ACTIVITY] Triggering AI coach for deal {deal_id}")
    
    try:
        recommendation = await ai_coach_service.get_recommendation(
            deal_id=deal_id,
            context=context
        )
        
        logger.info(
            f"[ACTIVITY] AI recommendation: {recommendation.suggested_action} "
            f"(confidence: {recommendation.confidence_score})"
        )
        
        return recommendation
        
    except Exception as e:
        logger.error(f"[ACTIVITY] AI coach failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def compensate_1c_rollback_activity(
    compensation: Dict[str, Any],
    integration_1c: Integration1CService
) -> bool:
    """
    Activity: Compensating transaction for Saga pattern
    Rolls back 1C operations on workflow failure
    
    Integrates with:
    - src/integrations/integration_1c.py
    - 1C document reversal
    """
    logger.warning(f"[ACTIVITY] Compensating 1C operation: {compensation.get('action')}")
    
    try:
        success = await integration_1c.rollback_document(
            document_id=compensation.get('entity_id'),
            reason=compensation.get('reason')
        )
        
        logger.warning(f"[ACTIVITY] Compensation completed: success={success}")
        return success
        
    except Exception as e:
        logger.error(f"[ACTIVITY] Compensation failed: {str(e)}")
        raise ApplicationError(str(e))


@activity.defn
async def persist_event_activity(
    event: Dict[str, Any],
    event_store: PostgreSQLEventStore
) -> bool:
    """
    Activity: Persist event to Event Store
    Ensures audit trail immutability
    """
    try:
        from src.core.models import DealEvent
        
        deal_event = DealEvent(**event)
        success = await event_store.append_event(deal_event)
        
        logger.debug(f"Event persisted: {event.get('event_type')} for deal {event.get('deal_id')}")
        return success
        
    except Exception as e:
        logger.error(f"Event persistence failed: {str(e)}")
        raise


# ============================================================================
# MAIN WORKFLOW - Deal Lifecycle
# ============================================================================

@workflow.defn
class DealLifecycleWorkflow:
    """
    Production-ready distributed workflow for deal lifecycle management
    
    Flow:
    1. Lead Qualification (via AI scoring engine)
    2. Proposal Generation (via LLM)
    3. 1C Sync (via HTTP API) - Saga pattern
    4. Response Waiting (with timeout and AI coach)
    5. Payment Tracking (via payment gateway)
    6. Deal Closure
    
    Features:
    - Event Sourcing for audit trail
    - Saga pattern for distributed transactions
    - Retry policies with exponential backoff
    - AI coaching on stuck deals
    - Real-time monitoring and notifications
    """

    def __init__(self):
        self.deal: Optional[Deal] = None
        self.compensations: List[Dict[str, Any]] = []
        self._response_received = False

    @workflow.run
    async def execute(
        self,
        deal_id: str,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str,
        # Dependency injection of services
        lead_scoring_engine: LeadScoringEngine,
        proposal_generator: ProposalGeneratorService,
        integration_1c: Integration1CService,
        payment_gateway: PaymentGatewayService,
        notification_service: NotificationService,
        ai_coach_service: AiCoachService,
        event_store: PostgreSQLEventStore
    ) -> Dict[str, Any]:
        """Main workflow execution"""

        logger.info(f"[WORKFLOW] Starting deal lifecycle for {deal_id}")

        # Initialize deal aggregate
        self.deal = Deal(
            deal_id=deal_id,
            status=DealStatus.LEAD,
            company=company_data,
            primary_contact=contact_info,
            requirements=requirements,
            workflow_run_id=workflow.info().workflow_id
        )

        # Retry policy: exponential backoff (1s, 2s, 4s, 8s)
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=8),
            maximum_attempts=4
        )

        try:
            # ===== STEP 1: QUALIFY LEAD =====
            logger.info(f"[WORKFLOW] Step 1: Qualifying lead {deal_id}")
            
            qual_result = await workflow.execute_activity(
                qualify_lead_activity,
                lead_id=deal_id,
                company_data=company_data,
                lead_scoring_engine=lead_scoring_engine,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy
            )

            # Log event
            self.deal.add_event(
                "LEAD_QUALIFIED",
                {
                    "score": qual_result.score,
                    "risk_level": qual_result.risk_level,
                    "reason": qual_result.reason
                },
                actor="QUALIFICATION_ENGINE"
            )

            # Persist event to Event Store
            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30)
            )

            if not qual_result.is_qualified:
                self.deal.transition_to(DealStatus.LOST, f"Lead rejected: {qual_result.reason}")
                logger.info(f"[WORKFLOW] Lead {deal_id} rejected")
                
                await workflow.execute_activity(
                    persist_event_activity,
                    event=self.deal.events[-1].dict(),
                    event_store=event_store,
                    start_to_close_timeout=timedelta(seconds=30)
                )

                return {
                    "deal_id": deal_id,
                    "status": DealStatus.LOST,
                    "reason": qual_result.reason,
                    "events": len(self.deal.events)
                }

            self.deal.transition_to(DealStatus.QUALIFIED, "Passed qualification")

            # ===== STEP 2: GENERATE PROPOSAL =====
            logger.info(f"[WORKFLOW] Step 2: Generating proposal for {deal_id}")

            proposal = await workflow.execute_activity(
                generate_proposal_activity,
                deal_id=deal_id,
                company_data=company_data,
                contact_info=contact_info,
                requirements=requirements,
                proposal_generator=proposal_generator,
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=retry_policy
            )

            self.deal.proposal_id = proposal.proposal_id
            self.deal.estimated_value = proposal.estimated_value

            self.deal.add_event(
                "PROPOSAL_GENERATED",
                {
                    "proposal_id": proposal.proposal_id,
                    "estimated_value": proposal.estimated_value,
                    "validity_days": proposal.validity_days
                },
                actor="PROPOSAL_ENGINE"
            )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30)
            )

            # ===== STEP 3: SAGA - SYNC WITH 1C =====
            logger.info(f"[WORKFLOW] Step 3: Syncing proposal to 1C")

            try:
                sync_result = await workflow.execute_activity(
                    sync_proposal_to_1c_activity,
                    deal_id=deal_id,
                    proposal_id=proposal.proposal_id,
                    company_id=company_data.get("id"),
                    estimated_value=proposal.estimated_value,
                    integration_1c=integration_1c,
                    start_to_close_timeout=timedelta(seconds=90),
                    retry_policy=retry_policy
                )

                self.deal.add_event(
                    "1C_SYNCED",
                    {
                        "1c_document_id": sync_result.get("1c_document_id"),
                        "1c_doc_number": sync_result.get("1c_doc_number")
                    },
                    actor="1C_INTEGRATION"
                )

                await workflow.execute_activity(
                    persist_event_activity,
                    event=self.deal.events[-1].dict(),
                    event_store=event_store,
                    start_to_close_timeout=timedelta(seconds=30)
                )

                # Store compensation for saga
                self.compensations.append({
                    "action": "rollback_1c",
                    "entity_id": sync_result.get("1c_document_id"),
                    "reason": "Saga compensation",
                    "external_system": "1C"
                })

            except ApplicationError as e:
                logger.error(f"[WORKFLOW] 1C sync failed: {str(e)}")
                self.deal.transition_to(DealStatus.LOST, "1C integration failure")
                
                # Execute compensations
                await self._execute_compensations(
                    integration_1c, event_store
                )
                
                raise

            self.deal.transition_to(DealStatus.PROPOSAL_SENT, "Proposal synced to 1C")

            # ===== STEP 4: WAIT FOR RESPONSE =====
            logger.info(f"[WORKFLOW] Step 4: Waiting for response")

            response_timeout = timedelta(days=14)
            
            try:
                await workflow.wait_condition(
                    lambda: self._response_received,
                    timeout=response_timeout
                )
                
                self.deal.add_event(
                    "RESPONSE_RECEIVED",
                    {"received_at": datetime.utcnow().isoformat()},
                    actor="EXTERNAL_SYSTEM"
                )
                
            except asyncio.TimeoutError:
                logger.warning(f"[WORKFLOW] No response for {response_timeout.days} days")
                
                self.deal.add_event(
                    "RESPONSE_TIMEOUT",
                    {"timeout_days": response_timeout.days},
                    actor="WORKFLOW_ENGINE"
                )

                # Send follow-up notification
                await workflow.execute_activity(
                    send_notification_activity,
                    deal_id=deal_id,
                    recipient=contact_info.get("email"),
                    notification_type="follow_up_reminder",
                    message="Reminder: Your proposal is expiring soon. Please confirm your interest.",
                    notification_service=notification_service,
                    start_to_close_timeout=timedelta(seconds=30)
                )

                # Transition to STUCK
                self.deal.transition_to(
                    DealStatus.STUCK,
                    "No response after 14 days"
                )

                # Get AI coach recommendation
                coach_rec = await workflow.execute_activity(
                    trigger_ai_coach_activity,
                    deal_id=deal_id,
                    context={
                        "status": "stuck",
                        "days_without_response": 14,
                        "company": company_data.get("name")
                    },
                    ai_coach_service=ai_coach_service,
                    start_to_close_timeout=timedelta(seconds=60)
                )

                self.deal.add_event(
                    "AI_COACHING_TRIGGERED",
                    {
                        "recommendation": coach_rec.suggested_action,
                        "urgency": coach_rec.urgency,
                        "confidence": coach_rec.confidence_score
                    },
                    actor="AI_COACH"
                )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30)
            )

            # ===== STEP 5: PAYMENT TRACKING =====
            logger.info(f"[WORKFLOW] Step 5: Tracking payment")

            payment_result = await workflow.execute_activity(
                check_payment_status_activity,
                deal_id=deal_id,
                expected_amount=proposal.estimated_value,
                payment_gateway=payment_gateway,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy
            )

            self.deal.add_event(
                "PAYMENT_CHECKED",
                {
                    "status": payment_result.payment_status,
                    "amount_due": payment_result.amount_due,
                    "overdue_days": payment_result.days_overdue,
                    "risk_score": payment_result.risk_score
                },
                actor="PAYMENT_GATEWAY"
            )

            if payment_result.days_overdue > 0:
                await workflow.execute_activity(
                    send_notification_activity,
                    deal_id=deal_id,
                    recipient="finance@company.com",
                    notification_type="payment_overdue",
                    message=f"Payment overdue by {payment_result.days_overdue} days for deal {deal_id}",
                    notification_service=notification_service,
                    start_to_close_timeout=timedelta(seconds=30)
                )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30)
            )

            # ===== FINAL STATUS =====
            if payment_result.payment_status == "COMPLETED":
                self.deal.transition_to(DealStatus.WON, "Payment received")
                self.deal.actual_value = payment_result.amount_paid
                
                logger.info(f"[WORKFLOW] Deal {deal_id} WON!")

            elif self.deal.status != DealStatus.STUCK:
                self.deal.transition_to(
                    DealStatus.NEGOTIATION,
                    "Awaiting full payment"
                )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30)
            )

            # Save final snapshot
            from src.core.models import DealSnapshot
            snapshot = DealSnapshot(
                deal_id=self.deal.deal_id,
                status=self.deal.status,
                current_state=self.deal.dict(),
                version=self.deal.version
            )
            await event_store.save_snapshot(snapshot)

            return {
                "deal_id": self.deal.deal_id,
                "final_status": self.deal.status,
                "workflow_run_id": workflow.info().workflow_id,
                "events_count": len(self.deal.events),
                "estimated_value": self.deal.estimated_value,
                "actual_value": self.deal.actual_value
            }

        except Exception as e:
            logger.error(f"[WORKFLOW] Deal workflow failed: {str(e)}")
            self.deal.transition_to(DealStatus.LOST, f"Workflow error: {str(e)}")
            await self._execute_compensations(integration_1c, event_store)
            raise

    @workflow.signal
    def mark_response_received(self):
        """Signal: external system notified of response"""
        self._response_received = True
        logger.info(f"[SIGNAL] Response received for deal {self.deal.deal_id if self.deal else 'unknown'}")

    async def _execute_compensations(
        self,
        integration_1c: Integration1CService,
        event_store: PostgreSQLEventStore
    ):
        """Execute compensating transactions for failed Saga steps"""
        logger.warning(f"[COMPENSATION] Executing {len(self.compensations)} compensations")

        for compensation in reversed(self.compensations):
            try:
                await workflow.execute_activity(
                    compensate_1c_rollback_activity,
                    compensation=compensation,
                    integration_1c=integration_1c,
                    start_to_close_timeout=timedelta(seconds=60)
                )

                self.deal.add_event(
                    "COMPENSATION_EXECUTED",
                    {
                        "action": compensation.get("action"),
                        "entity_id": compensation.get("entity_id")
                    },
                    actor="SAGA_ENGINE"
                )

                await workflow.execute_activity(
                    persist_event_activity,
                    event=self.deal.events[-1].dict(),
                    event_store=event_store,
                    start_to_close_timeout=timedelta(seconds=30)
                )

            except Exception as e:
                logger.error(f"[COMPENSATION] Failed: {str(e)}")
                self.deal.add_event(
                    "COMPENSATION_FAILED",
                    {
                        "action": compensation.get("action"),
                        "error": str(e)
                    },
                    actor="SAGA_ENGINE"
                )


# ============================================================================
# WORKFLOW CLIENT - Orchestrator
# ============================================================================

class WorkflowOrchestrator:
    """
    Production-ready Temporal workflow client
    Manages workflow lifecycle, signals, and monitoring
    """

    def __init__(
        self,
        temporal_server_host: str = "localhost:7233",
        namespace: str = "default"
    ):
        self.server_host = temporal_server_host
        self.namespace = namespace
        self.client: Optional[Client] = None

    async def initialize(self):
        """Initialize Temporal client connection"""
        try:
            self.client = await Client.connect(
                self.server_host,
                namespace=self.namespace
            )
            logger.info(f"Connected to Temporal at {self.server_host}")
        except Exception as e:
            logger.error(f"Failed to connect to Temporal: {str(e)}")
            raise

    async def start_deal_workflow(
        self,
        deal_id: str,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str,
        # Services
        lead_scoring_engine: LeadScoringEngine,
        proposal_generator: ProposalGeneratorService,
        integration_1c: Integration1CService,
        payment_gateway: PaymentGatewayService,
        notification_service: NotificationService,
        ai_coach_service: AiCoachService,
        event_store: PostgreSQLEventStore,
        # Workflow options
        timeout_seconds: int = 86400 * 30  # 30 days
    ) -> str:
        """
        Start a deal lifecycle workflow
        
        Args:
            deal_id: Unique deal identifier
            company_data: Company information
            contact_info: Primary contact details
            requirements: Deal requirements
            Services: All required service implementations
            timeout_seconds: Workflow timeout
        
        Returns:
            workflow_run_id: Unique identifier for this workflow execution
        """
        if not self.client:
            await self.initialize()

        workflow_run_id = f"deal-{deal_id}-{uuid.uuid4().hex[:8]}"

        try:
            await self.client.execute_workflow(
                DealLifecycleWorkflow.execute,
                deal_id=deal_id,
                company_data=company_data,
                contact_info=contact_info,
                requirements=requirements,
                lead_scoring_engine=lead_scoring_engine,
                proposal_generator=proposal_generator,
                integration_1c=integration_1c,
                payment_gateway=payment_gateway,
                notification_service=notification_service,
                ai_coach_service=ai_coach_service,
                event_store=event_store,
                id=workflow_run_id,
                task_queue="deal_processing",
                execution_timeout=timedelta(seconds=timeout_seconds),
                workflow_close_timeout=timedelta(seconds=600)
            )

            logger.info(f"Started workflow {workflow_run_id} for deal {deal_id}")
            return workflow_run_id

        except WorkflowAlreadyStartedError:
            logger.warning(f"Workflow {workflow_run_id} already started")
            raise
        except Exception as e:
            logger.error(f"Failed to start workflow: {str(e)}")
            raise

    async def get_workflow_status(self, workflow_run_id: str) -> Dict[str, Any]:
        """Get current workflow execution status"""
        if not self.client:
            raise RuntimeError("Orchestrator not initialized")

        try:
            handle = self.client.get_workflow_handle(workflow_run_id)
            desc = await handle.describe()

            return {
                "workflow_id": desc.workflow_id,
                "run_id": desc.run_id,
                "status": desc.status.name,
                "start_time": desc.start_time.isoformat(),
                "close_time": desc.close_time.isoformat() if desc.close_time else None,
                "execution_duration": (
                    (desc.close_time - desc.start_time).total_seconds()
                    if desc.close_time else None
                )
            }

        except Exception as e:
            logger.error(f"Failed to get workflow status: {str(e)}")
            raise

    async def get_workflow_result(self, workflow_run_id: str) -> Dict[str, Any]:
        """Get workflow execution result"""
        if not self.client:
            raise RuntimeError("Orchestrator not initialized")

        try:
            handle = self.client.get_workflow_handle(workflow_run_id)
            result = await handle.result()
            return result

        except Exception as e:
            logger.error(f"Failed to get workflow result: {str(e)}")
            raise

    async def signal_response_received(self, workflow_run_id: str) -> bool:
        """
        Send signal: External system confirmed response
        
        Usage:
        - When Bitrix24 confirms deal response
        - When customer replies to proposal
        - When payment is received
        """
        if not self.client:
            raise RuntimeError("Orchestrator not initialized")

        try:
            handle = self.client.get_workflow_handle(workflow_run_id)
            await handle.signal(DealLifecycleWorkflow.mark_response_received)

            logger.info(f"Signaled response_received to {workflow_run_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to signal workflow: {str(e)}")
            raise

    async def list_workflows(self, query: str = "") -> List[Dict[str, Any]]:
        """List all workflows (with optional filtering)"""
        if not self.client:
            raise RuntimeError("Orchestrator not initialized")

        try:
            # List active workflows
            workflows = []
            async for execution in self.client.list_workflows(query=query):
                workflows.append({
                    "workflow_id": execution.workflow_id,
                    "run_id": execution.run_id,
                    "type": execution.type,
                    "start_time": execution.start_time.isoformat(),
                    "status": "RUNNING"
                })

            return workflows

        except Exception as e:
            logger.error(f"Failed to list workflows: {str(e)}")
            raise

    async def close(self):
        """Close Temporal client connection"""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("Orchestrator closed")
