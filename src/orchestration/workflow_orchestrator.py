import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowAlreadyStartedError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from src.core.models import (
    AiCoachRecommendation,
    CompanyData,
    ContactInfo,
    Deal,
    DealStatus,
    LeadQualificationResult,
    Notification,
    PaymentCheckResult,
    PaymentStatus,
    ProposalGenerationResult,
)
from src.infrastructure.event_store import PostgreSQLEventStore
from src.integrations.ai_coach import AiCoachService
from src.integrations.integration_1c import Integration1CService
from src.integrations.lead_scoring import LeadScoringEngine
from src.integrations.notification_service import NotificationService
from src.integrations.payment_gateway import PaymentGatewayService
from src.integrations.proposal_generator import ProposalGeneratorService

logger = logging.getLogger(__name__)


@activity.defn
async def qualify_lead_activity(
    lead_id: str,
    company_data: Dict[str, Any],
    lead_scoring_engine: LeadScoringEngine,
) -> LeadQualificationResult:
    try:
        return await lead_scoring_engine.score_lead(
            lead_id=lead_id,
            company_data=company_data,
        )
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("Lead qualification failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def generate_proposal_activity(
    deal_id: str,
    company_data: Dict[str, Any],
    contact_info: Dict[str, Any],
    requirements: str,
    proposal_generator: ProposalGeneratorService,
) -> ProposalGenerationResult:
    try:
        return await proposal_generator.generate_proposal(
            deal_id=deal_id,
            company_data=company_data,
            contact_info=contact_info,
            requirements=requirements,
        )
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("Proposal generation failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def sync_proposal_to_1c_activity(
    deal_id: str,
    proposal_id: str,
    company_id: str,
    estimated_value: float,
    integration_1c: Integration1CService,
) -> Dict[str, Any]:
    try:
        return await integration_1c.create_proposal_document(
            deal_id=deal_id,
            proposal_id=proposal_id,
            company_id=company_id,
            estimated_value=estimated_value,
        )
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("1C sync failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def check_payment_status_activity(
    deal_id: str,
    expected_amount: float,
    payment_gateway: PaymentGatewayService,
) -> PaymentCheckResult:
    try:
        return await payment_gateway.check_payment(
            deal_id=deal_id,
            expected_amount=expected_amount,
        )
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("Payment check failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def send_notification_activity(
    deal_id: str,
    recipient: str,
    notification_type: str,
    message: str,
    notification_service: NotificationService,
) -> Dict[str, Any]:
    try:
        notification = Notification(
            deal_id=deal_id,
            recipient=recipient,
            notification_type=notification_type,
            message=message,
        )
        return await notification_service.send(notification)
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("Notification send failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def trigger_ai_coach_activity(
    deal_id: str,
    context: Dict[str, Any],
    ai_coach_service: AiCoachService,
) -> AiCoachRecommendation:
    try:
        return await ai_coach_service.get_recommendation(
            deal_id=deal_id,
            context=context,
        )
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("AI coach failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def compensate_1c_rollback_activity(
    compensation: Dict[str, Any],
    integration_1c: Integration1CService,
) -> bool:
    try:
        return await integration_1c.rollback_document(
            document_id=compensation.get("entity_id"),
            reason=compensation.get("reason"),
        )
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("Compensation rollback failed")
        raise ApplicationError(str(exc)) from exc


@activity.defn
async def persist_event_activity(
    event: Dict[str, Any],
    event_store: PostgreSQLEventStore,
) -> bool:
    try:
        from src.core.models import DealEvent

        deal_event = DealEvent(**event)
        return await event_store.append_event(deal_event)
    except Exception as exc:  # pragma: no cover - workflow error path
        logger.exception("Persist event failed")
        raise


@workflow.defn
class DealLifecycleWorkflow:
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
        lead_scoring_engine: LeadScoringEngine,
        proposal_generator: ProposalGeneratorService,
        integration_1c: Integration1CService,
        payment_gateway: PaymentGatewayService,
        notification_service: NotificationService,
        ai_coach_service: AiCoachService,
        event_store: PostgreSQLEventStore,
    ) -> Dict[str, Any]:
        logger.info("[WORKFLOW] Starting deal lifecycle for %s", deal_id)

        company_model = (
            company_data if isinstance(company_data, CompanyData) else CompanyData(**company_data)
        )
        contact_model = (
            contact_info if isinstance(contact_info, ContactInfo) else ContactInfo(**contact_info)
        )

        self.deal = Deal(
            deal_id=deal_id,
            status=DealStatus.LEAD,
            company=company_model,
            primary_contact=contact_model,
            requirements=requirements,
            workflow_run_id=workflow.info().workflow_id,
        )

        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=8),
            maximum_attempts=4,
        )

        try:
            qual_result = await workflow.execute_activity(
                qualify_lead_activity,
                lead_id=deal_id,
                company_data=company_data,
                lead_scoring_engine=lead_scoring_engine,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
            )

            self.deal.add_event(
                "LEAD_QUALIFIED",
                {
                    "score": qual_result.score,
                    "risk_level": qual_result.risk_level,
                    "reason": qual_result.reason,
                },
                actor="QUALIFICATION_ENGINE",
            )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30),
            )

            if not qual_result.is_qualified:
                self.deal.transition_to(DealStatus.LOST, f"Lead rejected: {qual_result.reason}")
                logger.info("[WORKFLOW] Lead %s rejected", deal_id)
                await workflow.execute_activity(
                    persist_event_activity,
                    event=self.deal.events[-1].dict(),
                    event_store=event_store,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                return {
                    "deal_id": deal_id,
                    "status": DealStatus.LOST,
                    "reason": qual_result.reason,
                    "events": len(self.deal.events),
                }

            self.deal.transition_to(DealStatus.QUALIFIED, "Passed qualification")

            proposal = await workflow.execute_activity(
                generate_proposal_activity,
                deal_id=deal_id,
                company_data=company_data,
                contact_info=contact_info,
                requirements=requirements,
                proposal_generator=proposal_generator,
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=retry_policy,
            )

            self.deal.proposal_id = proposal.proposal_id
            self.deal.estimated_value = proposal.estimated_value
            self.deal.add_event(
                "PROPOSAL_GENERATED",
                {
                    "proposal_id": proposal.proposal_id,
                    "estimated_value": proposal.estimated_value,
                    "validity_days": proposal.validity_days,
                },
                actor="PROPOSAL_ENGINE",
            )
            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30),
            )

            try:
                sync_result = await workflow.execute_activity(
                    sync_proposal_to_1c_activity,
                    deal_id=deal_id,
                    proposal_id=proposal.proposal_id,
                    company_id=company_data.get("id"),
                    estimated_value=proposal.estimated_value,
                    integration_1c=integration_1c,
                    start_to_close_timeout=timedelta(seconds=90),
                    retry_policy=retry_policy,
                )

                self.deal.add_event(
                    "1C_SYNCED",
                    {
                        "1c_document_id": sync_result.get("1c_document_id"),
                        "1c_doc_number": sync_result.get("1c_doc_number"),
                    },
                    actor="1C_INTEGRATION",
                )
                await workflow.execute_activity(
                    persist_event_activity,
                    event=self.deal.events[-1].dict(),
                    event_store=event_store,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                self.compensations.append(
                    {
                        "action": "rollback_1c",
                        "entity_id": sync_result.get("1c_document_id"),
                        "reason": "Saga compensation",
                        "external_system": "1C",
                    }
                )
            except ApplicationError as exc:
                logger.exception("[WORKFLOW] 1C sync failed")
                self.deal.transition_to(DealStatus.LOST, "1C integration failure")
                await self._execute_compensations(integration_1c, event_store)
                raise exc

            self.deal.transition_to(DealStatus.PROPOSAL_SENT, "Proposal synced to 1C")

            try:
                await workflow.wait_condition(
                    lambda: self._response_received,
                    timeout=timedelta(days=14),
                )
                self.deal.add_event(
                    "RESPONSE_RECEIVED",
                    {"received_at": datetime.utcnow().isoformat()},
                    actor="EXTERNAL_SYSTEM",
                )
            except asyncio.TimeoutError:
                logger.warning("[WORKFLOW] No response for 14 days")
                self.deal.add_event(
                    "RESPONSE_TIMEOUT",
                    {"timeout_days": 14},
                    actor="WORKFLOW_ENGINE",
                )
                await workflow.execute_activity(
                    send_notification_activity,
                    deal_id=deal_id,
                    recipient=contact_info.get("email"),
                    notification_type="follow_up_reminder",
                    message="Reminder: Your proposal is expiring soon. Please confirm your interest.",
                    notification_service=notification_service,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                self.deal.transition_to(DealStatus.STUCK, "No response after 14 days")
                coach_rec = await workflow.execute_activity(
                    trigger_ai_coach_activity,
                    deal_id=deal_id,
                    context={
                        "status": "stuck",
                        "days_without_response": 14,
                        "company": company_data.get("name"),
                    },
                    ai_coach_service=ai_coach_service,
                    start_to_close_timeout=timedelta(seconds=60),
                )
                self.deal.add_event(
                    "AI_COACHING_TRIGGERED",
                    {
                        "recommendation": coach_rec.suggested_action,
                        "urgency": coach_rec.urgency,
                        "confidence": coach_rec.confidence_score,
                    },
                    actor="AI_COACH",
                )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30),
            )

            payment_result = await workflow.execute_activity(
                check_payment_status_activity,
                deal_id=deal_id,
                expected_amount=proposal.estimated_value,
                payment_gateway=payment_gateway,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
            )

            self.deal.add_event(
                "PAYMENT_CHECKED",
                {
                    "status": payment_result.payment_status,
                    "amount_due": payment_result.amount_due,
                    "overdue_days": payment_result.days_overdue,
                    "risk_score": payment_result.risk_score,
                },
                actor="PAYMENT_GATEWAY",
            )

            if payment_result.days_overdue > 0:
                await workflow.execute_activity(
                    send_notification_activity,
                    deal_id=deal_id,
                    recipient="finance@company.com",
                    notification_type="payment_overdue",
                    message=f"Payment overdue by {payment_result.days_overdue} days for deal {deal_id}",
                    notification_service=notification_service,
                    start_to_close_timeout=timedelta(seconds=30),
                )

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30),
            )

            if payment_result.payment_status == PaymentStatus.COMPLETED:
                self.deal.transition_to(DealStatus.WON, "Payment received")
                self.deal.actual_value = payment_result.amount_paid
                logger.info("[WORKFLOW] Deal %s WON!", deal_id)
            elif self.deal.status != DealStatus.STUCK:
                self.deal.transition_to(DealStatus.NEGOTIATION, "Awaiting full payment")

            await workflow.execute_activity(
                persist_event_activity,
                event=self.deal.events[-1].dict(),
                event_store=event_store,
                start_to_close_timeout=timedelta(seconds=30),
            )

            from src.core.models import DealSnapshot

            snapshot = DealSnapshot(
                deal_id=self.deal.deal_id,
                status=self.deal.status,
                current_state=self.deal.dict(),
                version=self.deal.version,
            )
            await event_store.save_snapshot(snapshot)

            return {
                "deal_id": self.deal.deal_id,
                "final_status": self.deal.status,
                "workflow_run_id": workflow.info().workflow_id,
                "events_count": len(self.deal.events),
                "estimated_value": self.deal.estimated_value,
                "actual_value": self.deal.actual_value,
            }

        except Exception as exc:
            logger.exception("[WORKFLOW] Deal workflow failed")
            if self.deal is not None:
                self.deal.transition_to(DealStatus.LOST, f"Workflow error: {exc}")
            await self._execute_compensations(integration_1c, event_store)
            raise

    @workflow.signal
    def mark_response_received(self):
        self._response_received = True
        logger.info(
            "[SIGNAL] Response received for deal %s",
            self.deal.deal_id if self.deal else "unknown",
        )

    async def _execute_compensations(
        self,
        integration_1c: Integration1CService,
        event_store: PostgreSQLEventStore,
    ):
        logger.warning("[COMPENSATION] Executing %s compensations", len(self.compensations))
        for compensation in reversed(self.compensations):
            try:
                await workflow.execute_activity(
                    compensate_1c_rollback_activity,
                    compensation=compensation,
                    integration_1c=integration_1c,
                    start_to_close_timeout=timedelta(seconds=60),
                )
                self.deal.add_event(
                    "COMPENSATION_EXECUTED",
                    {
                        "action": compensation.get("action"),
                        "entity_id": compensation.get("entity_id"),
                    },
                    actor="SAGA_ENGINE",
                )
                await workflow.execute_activity(
                    persist_event_activity,
                    event=self.deal.events[-1].dict(),
                    event_store=event_store,
                    start_to_close_timeout=timedelta(seconds=30),
                )
            except Exception as exc:  # pragma: no cover - workflow recovery path
                logger.exception("Compensation failed")
                self.deal.add_event(
                    "COMPENSATION_FAILED",
                    {
                        "action": compensation.get("action"),
                        "error": str(exc),
                    },
                    actor="SAGA_ENGINE",
                )


class WorkflowOrchestrator:
    def __init__(
        self,
        temporal_server_host: str = "localhost:7233",
        namespace: str = "default",
    ):
        self.server_host = temporal_server_host
        self.namespace = namespace
        self.client: Optional[Client] = None

    async def initialize(self):
        self.client = await Client.connect(
            self.server_host,
            namespace=self.namespace,
        )
        logger.info("Connected to Temporal at %s", self.server_host)

    async def start_deal_workflow(
        self,
        deal_id: str,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str,
        lead_scoring_engine: LeadScoringEngine,
        proposal_generator: ProposalGeneratorService,
        integration_1c: Integration1CService,
        payment_gateway: PaymentGatewayService,
        notification_service: NotificationService,
        ai_coach_service: AiCoachService,
        event_store: PostgreSQLEventStore,
        timeout_seconds: int = 86400 * 30,
    ) -> str:
        if self.client is None:
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
                workflow_close_timeout=timedelta(seconds=600),
            )
            logger.info("Started workflow %s for deal %s", workflow_run_id, deal_id)
            return workflow_run_id
        except WorkflowAlreadyStartedError:
            logger.warning("Workflow %s already started", workflow_run_id)
            raise
        except Exception as exc:
            logger.exception("Failed to start workflow")
            raise exc

    async def get_workflow_status(self, workflow_run_id: str) -> Dict[str, Any]:
        if self.client is None:
            raise RuntimeError("Orchestrator not initialized")

        handle = self.client.get_workflow_handle(workflow_run_id)
        desc = await handle.describe()
        return {
            "workflow_id": desc.workflow_id,
            "run_id": desc.run_id,
            "status": desc.status.name,
            "start_time": desc.start_time.isoformat(),
            "close_time": desc.close_time.isoformat() if desc.close_time else None,
            "execution_duration": (
                (desc.close_time - desc.start_time).total_seconds() if desc.close_time else None
            ),
        }

    async def get_workflow_result(self, workflow_run_id: str) -> Dict[str, Any]:
        if self.client is None:
            raise RuntimeError("Orchestrator not initialized")

        handle = self.client.get_workflow_handle(workflow_run_id)
        return await handle.result()

    async def signal_response_received(self, workflow_run_id: str) -> bool:
        if self.client is None:
            raise RuntimeError("Orchestrator not initialized")

        handle = self.client.get_workflow_handle(workflow_run_id)
        await handle.signal(DealLifecycleWorkflow.mark_response_received)
        logger.info("Signaled response_received to %s", workflow_run_id)
        return True

    async def list_workflows(self, query: str = "") -> List[Dict[str, Any]]:
        if self.client is None:
            raise RuntimeError("Orchestrator not initialized")

        workflows: List[Dict[str, Any]] = []
        async for execution in self.client.list_workflows(query=query):
            workflows.append(
                {
                    "workflow_id": execution.workflow_id,
                    "run_id": execution.run_id,
                    "type": execution.type,
                    "start_time": execution.start_time.isoformat(),
                    "status": "RUNNING",
                }
            )
        return workflows

    async def close(self):
        if self.client is not None:
            await self.client.aclose()
            self.client = None
            logger.info("Orchestrator closed")
