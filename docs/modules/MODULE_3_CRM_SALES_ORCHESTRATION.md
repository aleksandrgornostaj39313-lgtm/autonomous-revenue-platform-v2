# MODULE 3: CRM / SALES ORCHESTRATION
## Production-grade orchestration of customer lifecycle, CRM synchronization, and sales execution

```python
# src/crm/models.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class DealStage(str, Enum):
    LEAD = "lead"
    QUALIFIED = "qualified"
    PROPOSAL_SENT = "proposal_sent"
    NEGOTIATION = "negotiation"
    READY_TO_SIGN = "ready_to_sign"
    WON = "won"
    LOST = "lost"
    STUCK = "stuck"


class ActivityType(str, Enum):
    CALL = "call"
    EMAIL = "email"
    MEETING = "meeting"
    TASK = "task"
    NOTE = "note"
    STATUS_CHANGE = "status_change"


@dataclass
class Contact:
    id: str
    company_id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    seniority: Optional[str] = None
    is_primary: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Company:
    id: str
    name: str
    industry: Optional[str] = None
    region: Optional[str] = None
    employee_count: Optional[int] = None
    revenue: Optional[float] = None
    source: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Deal:
    id: str
    company_id: str
    owner_id: str
    title: str
    stage: DealStage
    amount: float
    probability: float
    expected_close_date: Optional[datetime] = None
    description: Optional[str] = None
    source: Optional[str] = None
    status_reason: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    activity_history: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)


@dataclass
class ActivityLog:
    id: str
    deal_id: str
    contact_id: Optional[str]
    type: ActivityType
    channel: str
    summary: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SalesAssignment:
    id: str
    deal_id: str
    assignee_id: str
    assigned_by: str
    assigned_at: datetime = field(default_factory=datetime.utcnow)
    due_at: Optional[datetime] = None
    status: str = "active"
```

```python
# src/crm/repositories.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.crm.models import Company, Contact, Deal, ActivityLog


class CRMRepository(ABC):
    @abstractmethod
    async def save_company(self, company: Company) -> Company:
        raise NotImplementedError

    @abstractmethod
    async def get_company(self, company_id: str) -> Optional[Company]:
        raise NotImplementedError

    @abstractmethod
    async def save_contact(self, contact: Contact) -> Contact:
        raise NotImplementedError

    @abstractmethod
    async def save_deal(self, deal: Deal) -> Deal:
        raise NotImplementedError

    @abstractmethod
    async def get_deal(self, deal_id: str) -> Optional[Deal]:
        raise NotImplementedError

    @abstractmethod
    async def list_deals_by_stage(self, stage: str) -> List[Deal]:
        raise NotImplementedError

    @abstractmethod
    async def append_activity(self, activity: ActivityLog) -> ActivityLog:
        raise NotImplementedError


class PostgresCRMRepository(CRMRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def save_company(self, company: Company) -> Company:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO crm_companies (
                    id, name, industry, region, employee_count, revenue, source,
                    created_at, updated_at
                ) VALUES (
                    :id, :name, :industry, :region, :employee_count, :revenue, :source,
                    :created_at, :updated_at
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    industry = EXCLUDED.industry,
                    region = EXCLUDED.region,
                    employee_count = EXCLUDED.employee_count,
                    revenue = EXCLUDED.revenue,
                    source = EXCLUDED.source,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "id": company.id,
                    "name": company.name,
                    "industry": company.industry,
                    "region": company.region,
                    "employee_count": company.employee_count,
                    "revenue": company.revenue,
                    "source": company.source,
                    "created_at": company.created_at,
                    "updated_at": company.updated_at,
                },
            )
            await session.commit()
        return company

    async def get_company(self, company_id: str) -> Optional[Company]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                "SELECT * FROM crm_companies WHERE id = :company_id",
                {"company_id": company_id},
            )
        if row is None:
            return None
        return Company(
            id=row["id"],
            name=row["name"],
            industry=row["industry"],
            region=row["region"],
            employee_count=row["employee_count"],
            revenue=row["revenue"],
            source=row["source"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def save_contact(self, contact: Contact) -> Contact:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO crm_contacts (
                    id, company_id, name, email, phone, role, seniority, is_primary,
                    created_at, updated_at
                ) VALUES (
                    :id, :company_id, :name, :email, :phone, :role, :seniority, :is_primary,
                    :created_at, :updated_at
                )
                ON CONFLICT (id) DO UPDATE SET
                    company_id = EXCLUDED.company_id,
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    role = EXCLUDED.role,
                    seniority = EXCLUDED.seniority,
                    is_primary = EXCLUDED.is_primary,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "id": contact.id,
                    "company_id": contact.company_id,
                    "name": contact.name,
                    "email": contact.email,
                    "phone": contact.phone,
                    "role": contact.role,
                    "seniority": contact.seniority,
                    "is_primary": contact.is_primary,
                    "created_at": contact.created_at,
                    "updated_at": contact.updated_at,
                },
            )
            await session.commit()
        return contact

    async def save_deal(self, deal: Deal) -> Deal:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO crm_deals (
                    id, company_id, owner_id, title, stage, amount, probability,
                    expected_close_date, description, source, status_reason,
                    created_at, updated_at, tags
                ) VALUES (
                    :id, :company_id, :owner_id, :title, :stage, :amount, :probability,
                    :expected_close_date, :description, :source, :status_reason,
                    :created_at, :updated_at, :tags
                )
                ON CONFLICT (id) DO UPDATE SET
                    company_id = EXCLUDED.company_id,
                    owner_id = EXCLUDED.owner_id,
                    title = EXCLUDED.title,
                    stage = EXCLUDED.stage,
                    amount = EXCLUDED.amount,
                    probability = EXCLUDED.probability,
                    expected_close_date = EXCLUDED.expected_close_date,
                    description = EXCLUDED.description,
                    source = EXCLUDED.source,
                    status_reason = EXCLUDED.status_reason,
                    updated_at = EXCLUDED.updated_at,
                    tags = EXCLUDED.tags
                """,
                {
                    "id": deal.id,
                    "company_id": deal.company_id,
                    "owner_id": deal.owner_id,
                    "title": deal.title,
                    "stage": deal.stage.value,
                    "amount": deal.amount,
                    "probability": deal.probability,
                    "expected_close_date": deal.expected_close_date,
                    "description": deal.description,
                    "source": deal.source,
                    "status_reason": deal.status_reason,
                    "created_at": deal.created_at,
                    "updated_at": deal.updated_at,
                    "tags": deal.tags,
                },
            )
            await session.commit()
        return deal

    async def get_deal(self, deal_id: str) -> Optional[Deal]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                "SELECT * FROM crm_deals WHERE id = :deal_id",
                {"deal_id": deal_id},
            )
        if row is None:
            return None
        return Deal(
            id=row["id"],
            company_id=row["company_id"],
            owner_id=row["owner_id"],
            title=row["title"],
            stage=DealStage(row["stage"]),
            amount=row["amount"],
            probability=row["probability"],
            expected_close_date=row["expected_close_date"],
            description=row["description"],
            source=row["source"],
            status_reason=row["status_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            tags=row["tags"] or [],
        )

    async def list_deals_by_stage(self, stage: str) -> List[Deal]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                "SELECT * FROM crm_deals WHERE stage = :stage ORDER BY updated_at DESC",
                {"stage": stage},
            )
        return [
            Deal(
                id=row["id"],
                company_id=row["company_id"],
                owner_id=row["owner_id"],
                title=row["title"],
                stage=DealStage(row["stage"]),
                amount=row["amount"],
                probability=row["probability"],
                expected_close_date=row["expected_close_date"],
                description=row["description"],
                source=row["source"],
                status_reason=row["status_reason"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                tags=row["tags"] or [],
            )
            for row in rows
        ]

    async def append_activity(self, activity: ActivityLog) -> ActivityLog:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO crm_activities (
                    id, deal_id, contact_id, type, channel, summary, metadata, created_at
                ) VALUES (
                    :id, :deal_id, :contact_id, :type, :channel, :summary, :metadata, :created_at
                )
                """,
                {
                    "id": activity.id,
                    "deal_id": activity.deal_id,
                    "contact_id": activity.contact_id,
                    "type": activity.type.value,
                    "channel": activity.channel,
                    "summary": activity.summary,
                    "metadata": activity.metadata,
                    "created_at": activity.created_at,
                },
            )
            await session.commit()
        return activity
```

```python
# src/crm/integrations/base_adapter.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExternalCRMAdapter(ABC):
    @abstractmethod
    async def fetch_leads(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_deals(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def sync_company(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def sync_deal(self, deal_data: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class Bitrix24Adapter(ExternalCRMAdapter):
    def __init__(self, client: Any, api_version: str = "v2"):
        self.client = client
        self.api_version = api_version

    async def fetch_leads(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = {
            "filter": filters,
            "select": ["ID", "TITLE", "STATUS_ID", "DATE_CREATE", "OPPORTUNITY"],
        }
        response = await self.client.request("crm.lead.list", payload)
        return response.get("result", [])

    async def fetch_deals(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = {
            "filter": filters,
            "select": ["ID", "TITLE", "STAGE_ID", "OPPORTUNITY", "DATE_CREATE"],
        }
        response = await self.client.request("crm.deal.list", payload)
        return response.get("result", [])

    async def sync_company(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "fields": {
                "TITLE": company_data["name"],
                "INDUSTRY": company_data.get("industry"),
                "ADDRESS": company_data.get("region"),
            }
        }
        result = await self.client.request("crm.company.add", payload)
        return {"external_id": result["result"], "source": "bitrix24"}

    async def sync_deal(self, deal_data: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "fields": {
                "TITLE": deal_data["title"],
                "ASSIGNED_BY_ID": deal_data["owner_id"],
                "OPPORTUNITY": deal_data["amount"],
                "STAGE_ID": deal_data["stage"],
            }
        }
        result = await self.client.request("crm.deal.add", payload)
        return {"external_id": result["result"], "source": "bitrix24"}


class HubSpotAdapter(ExternalCRMAdapter):
    def __init__(self, client: Any):
        self.client = client

    async def fetch_leads(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = await self.client.crm.objects.search.run(
            object_type="contacts",
            filter_groups=[{"filters": [{"propertyName": k, "operator": "EQ", "value": v} for k, v in filters.items()]}],
        )
        return response.get("results", [])

    async def fetch_deals(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = await self.client.crm.deals.search.do_search({
            "filterGroups": [{"filters": [{"propertyName": k, "operator": "EQ", "value": v} for k, v in filters.items()]}]
        })
        return response.get("results", [])

    async def sync_company(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        response = await self.client.companies.basic_api.create({
            "properties": {
                "name": company_data["name"],
                "industry": company_data.get("industry", ""),
            }
        })
        return {"external_id": response["id"], "source": "hubspot"}

    async def sync_deal(self, deal_data: Dict[str, Any]) -> Dict[str, Any]:
        response = await self.client.crm.deals.basic_api.create({
            "associations": [],
            "properties": {
                "dealname": deal_data["title"],
                "amount": str(deal_data["amount"]),
                "dealstage": deal_data["stage"],
            },
        })
        return {"external_id": response["id"], "source": "hubspot"}
```

```python
# src/crm/sales_orchestrator.py

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.crm.integrations.base_adapter import ExternalCRMAdapter
from src.crm.models import ActivityLog, ActivityType, Contact, Deal, DealStage
from src.crm.repositories import CRMRepository

logger = logging.getLogger(__name__)


class SalesOrchestrator:
    def __init__(self, repository: CRMRepository, adapters: Dict[str, ExternalCRMAdapter]):
        self.repository = repository
        self.adapters = adapters

    async def sync_external_leads(self, source: str, filters: Dict[str, Any]) -> List[str]:
        adapter = self.adapters[source]
        rows = await adapter.fetch_leads(filters)
        synced_ids: List[str] = []

        for row in rows:
            company_id = row.get("company_id") or row.get("companyId") or f"company-{row.get('id')}"
            deal_id = row.get("deal_id") or row.get("dealId") or f"deal-{row.get('id')}"
            owner_id = row.get("owner_id") or "system-owner"

            company = await self.repository.get_company(company_id)
            if company is None:
                company = await self.repository.save_company(
                    Company(
                        id=company_id,
                        name=row.get("company_name") or row.get("companyName") or "Unknown",
                        industry=row.get("industry"),
                        region=row.get("region"),
                        employee_count=row.get("employee_count"),
                        revenue=row.get("revenue"),
                        source=source,
                    )
                )

            deal = await self.repository.get_deal(deal_id)
            if deal is None:
                deal = Deal(
                    id=deal_id,
                    company_id=company.id,
                    owner_id=owner_id,
                    title=row.get("title") or row.get("name") or "New Opportunity",
                    stage=DealStage.LEAD,
                    amount=float(row.get("opportunity", 0) or 0),
                    probability=float(row.get("probability", 0.0) or 0.0),
                    expected_close_date=row.get("expected_close_date"),
                    source=source,
                    description=row.get("description"),
                )
                await self.repository.save_deal(deal)
                synced_ids.append(deal_id)

        return synced_ids

    async def create_or_update_contact(self, contact: Contact) -> Contact:
        return await self.repository.save_contact(contact)

    async def on_new_activity(self, deal_id: str, activity_type: ActivityType, summary: str, channel: str, metadata: Dict[str, Any]) -> ActivityLog:
        activity = ActivityLog(
            id=f"activity-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
            deal_id=deal_id,
            contact_id=metadata.get("contact_id"),
            type=activity_type,
            channel=channel,
            summary=summary,
            metadata=metadata,
        )
        return await self.repository.append_activity(activity)

    async def assign_deal(self, deal_id: str, assignee_id: str, assigned_by: str, due_at: Optional[datetime] = None) -> Dict[str, Any]:
        deal = await self.repository.get_deal(deal_id)
        if deal is None:
            raise ValueError(f"Deal {deal_id} not found")

        assignment = {
            "deal_id": deal_id,
            "assignee_id": assignee_id,
            "assigned_by": assigned_by,
            "due_at": due_at or (datetime.utcnow() + timedelta(days=3)),
            "status": "active",
        }

        await self.on_new_activity(
            deal_id,
            ActivityType.TASK,
            f"Deal assigned to {assignee_id}",
            "crm",
            {"assignment": assignment},
        )

        deal.updated_at = datetime.utcnow()
        await self.repository.save_deal(deal)
        return assignment

    async def move_stage(self, deal_id: str, new_stage: DealStage, reason: str) -> Deal:
        deal = await self.repository.get_deal(deal_id)
        if deal is None:
            raise ValueError(f"Deal {deal_id} not found")

        deal.stage = new_stage
        deal.updated_at = datetime.utcnow()
        deal.status_reason = reason

        await self.repository.save_deal(deal)

        await self.on_new_activity(
            deal_id,
            ActivityType.STATUS_CHANGE,
            f"Stage changed to {new_stage.value}",
            "crm",
            {"reason": reason, "new_stage": new_stage.value},
        )

        return deal

    async def sync_to_external(self, source: str, deal: Deal) -> Dict[str, Any]:
        adapter = self.adapters.get(source)
        if adapter is None:
            raise ValueError(f"No adapter registered for source {source}")

        payload = {
            "title": deal.title,
            "owner_id": deal.owner_id,
            "amount": deal.amount,
            "stage": deal.stage.value,
        }
        response = await adapter.sync_deal(payload)
        return {
            "external_id": response["external_id"],
            "source": response["source"],
            "synced_at": datetime.utcnow().isoformat(),
        }
```

```python
# src/crm/sales_pipeline.py

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.crm.models import Deal, DealStage
from src.crm.repositories import CRMRepository

logger = logging.getLogger(__name__)


class SalesPipeline:
    def __init__(self, repository: CRMRepository):
        self.repository = repository

    async def pipeline_summary(self) -> Dict[str, Any]:
        stages = [
            DealStage.LEAD,
            DealStage.QUALIFIED,
            DealStage.PROPOSAL_SENT,
            DealStage.NEGOTIATION,
            DealStage.READY_TO_SIGN,
            DealStage.WON,
            DealStage.LOST,
            DealStage.STUCK,
        ]

        summary = {"stages": {stage.value: 0 for stage in stages}}
        revenue_by_stage = {stage.value: 0.0 for stage in stages}

        for stage in stages:
            deals = await self.repository.list_deals_by_stage(stage.value)
            summary["stages"][stage.value] = len(deals)
            revenue_by_stage[stage.value] = sum(deal.amount for deal in deals)

        summary["revenue_by_stage"] = revenue_by_stage
        summary["total_pipeline_value"] = sum(revenue_by_stage.values())
        summary["updated_at"] = datetime.utcnow().isoformat()
        return summary
```

```python
# src/crm/deal_workflow.py

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from src.crm.models import Deal, DealStage
from src.crm.repositories import CRMRepository

logger = logging.getLogger(__name__)


class DealWorkflowEngine:
    def __init__(self, repository: CRMRepository):
        self.repository = repository

    async def update_based_on_ai_result(self, deal_id: str, qualification: Dict[str, Any]) -> Deal:
        deal = await self.repository.get_deal(deal_id)
        if deal is None:
            raise ValueError(f"Deal {deal_id} not found")

        if qualification.get("qualified"):
            if deal.stage in {DealStage.LEAD, DealStage.QUALIFIED}:
                deal.stage = DealStage.QUALIFIED
        else:
            deal.stage = DealStage.LOST
            deal.status_reason = qualification.get("reason", "Not qualified by AI")

        deal.updated_at = datetime.utcnow()
        await self.repository.save_deal(deal)
        return deal

    async def follow_up_deadlines(self) -> List[Dict[str, Any]]:
        deals = []
        for stage in [DealStage.PROPOSAL_SENT, DealStage.NEGOTIATION, DealStage.READY_TO_SIGN, DealStage.STUCK]:
            rows = await self.repository.list_deals_by_stage(stage.value)
            deals.extend(rows)

        due_items = []
        for deal in deals:
            if deal.expected_close_date is None:
                continue
            delta = deal.expected_close_date - datetime.utcnow()
            if delta <= timedelta(days=7):
                due_items.append({
                    "deal_id": deal.id,
                    "company_id": deal.company_id,
                    "stage": deal.stage.value,
                    "days_left": max(delta.days, 0),
                    "amount": deal.amount,
                })

        return due_items
```

```python
# src/crm/service_layer.py

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.crm.deal_workflow import DealWorkflowEngine
from src.crm.models import ActivityType, Contact, Company, Deal, DealStage
from src.crm.repositories import CRMRepository
from src.crm.sales_orchestrator import SalesOrchestrator
from src.crm.sales_pipeline import SalesPipeline

logger = logging.getLogger(__name__)


class CRMService:
    def __init__(self, repository: CRMRepository, orchestrator: SalesOrchestrator):
        self.repository = repository
        self.orchestrator = orchestrator
        self.workflow = DealWorkflowEngine(repository)
        self.pipeline = SalesPipeline(repository)

    async def register_company(self, payload: Dict[str, Any]) -> Company:
        company = Company(
            id=payload["id"],
            name=payload["name"],
            industry=payload.get("industry"),
            region=payload.get("region"),
            employee_count=payload.get("employee_count"),
            revenue=payload.get("revenue"),
            source=payload.get("source"),
        )
        return await self.repository.save_company(company)

    async def register_contact(self, payload: Dict[str, Any]) -> Contact:
        contact = Contact(
            id=payload["id"],
            company_id=payload["company_id"],
            name=payload["name"],
            email=payload.get("email"),
            phone=payload.get("phone"),
            role=payload.get("role"),
            seniority=payload.get("seniority"),
            is_primary=payload.get("is_primary", False),
        )
        return await self.orchestrator.create_or_update_contact(contact)

    async def create_deal(self, payload: Dict[str, Any]) -> Deal:
        deal = Deal(
            id=payload["id"],
            company_id=payload["company_id"],
            owner_id=payload["owner_id"],
            title=payload["title"],
            stage=DealStage(payload.get("stage", DealStage.LEAD.value)),
            amount=float(payload.get("amount", 0.0)),
            probability=float(payload.get("probability", 0.0)),
            expected_close_date=payload.get("expected_close_date"),
            description=payload.get("description"),
            source=payload.get("source"),
            status_reason=payload.get("status_reason"),
        )
        created = await self.repository.save_deal(deal)
        await self.orchestrator.on_new_activity(
            created.id,
            ActivityType.NOTE,
            "Deal created in CRM",
            "crm",
            {"dealtitle": created.title, "amount": created.amount},
        )
        return created

    async def pipeline_snapshot(self) -> Dict[str, Any]:
        return await self.pipeline.pipeline_summary()

    async def action_required(self) -> List[Dict[str, Any]]:
        return await self.workflow.follow_up_deadlines()
```

```python
# src/crm/api_routes.py

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional

from src.crm.service_layer import CRMService

router = APIRouter(prefix="/api/v1/crm", tags=["crm"])


class CompanyPayload(BaseModel):
    id: str
    name: str
    industry: Optional[str] = None
    region: Optional[str] = None
    employee_count: Optional[int] = None
    revenue: Optional[float] = None
    source: Optional[str] = None


class ContactPayload(BaseModel):
    id: str
    company_id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    seniority: Optional[str] = None
    is_primary: bool = False


class DealPayload(BaseModel):
    id: str
    company_id: str
    owner_id: str
    title: str
    stage: Optional[str] = "lead"
    amount: float = 0.0
    probability: float = 0.0
    expected_close_date: Optional[str] = None
    description: Optional[str] = None
    source: Optional[str] = None
    status_reason: Optional[str] = None


@router.post("/companies")
async def register_company(payload: CompanyPayload, service: CRMService = Depends(get_service)):
    return await service.register_company(payload.model_dump())


@router.post("/contacts")
async def register_contact(payload: ContactPayload, service: CRMService = Depends(get_service)):
    return await service.register_contact(payload.model_dump())


@router.post("/deals")
async def create_deal(payload: DealPayload, service: CRMService = Depends(get_service)):
    return await service.create_deal(payload.model_dump())


@router.get("/pipeline")
async def pipeline_snapshot(service: CRMService = Depends(get_service)):
    return await service.pipeline_snapshot()


@router.get("/action-required")
async def action_required(service: CRMService = Depends(get_service)):
    return await service.action_required()
```

---

## Design principles for Module 3

1. CRM is not a storage layer only; it is the operational system of record for deal state.
2. Every external sync must use a canonical internal model and map to adapter-specific payloads.
3. Deal lifecycle state is enforced by workflow and CRM state machine, not by ad-hoc UI actions.
4. Sales activities are recorded as structured events, not as free-form comments only.
5. Each deal has a single owning team and clear assignment history.
6. Pipeline metrics are derived from internal state, not from stale integration snapshots.

---

## Integration map

```text
Bitrix24 / HubSpot / 1C
        │
        ▼
External CRM adapter layer
        │
        ▼
Canonical CRM service
        │
        ├── repository (PostgreSQL)
        ├── deal workflow engine
        ├── sales pipeline
        └── activity/event log
```

---

## Operational rules

### 1. Single source of truth
The internal platform owns the current deal stage and assignment records. Integration updates are applied only through the synchronization service and must not overwrite the authoritative stage without conflict resolution.

### 2. Conflict resolution
When external CRM and internal platform disagree on stage or amount:
- compare timestamps;
- prefer the later authoritative event;
- preserve both values in an audit trail;
- if the conflict is business-critical, escalate to a human review queue.

### 3. Activity logging
Every sales event must be persisted as a structured record with metadata:
- type;
- channel;
- summary;
- timestamp;
- link to deal and contact;
- actor id.

### 4. Assignment policy
Assignments are immutable in their operational intent: the system stores the assignment creation, current assignee, and due date, then audits changes.

### 5. Pipeline health
Deals that remain stuck for more than a SLA threshold trigger:
- AI coach recommendation;
- manager review;
- escalation or re-qualification.

---

## Production-ready synchronization flow

```python
# src/crm/sync_service.py

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.crm.integrations.base_adapter import ExternalCRMAdapter
from src.crm.models import Deal, DealStage
from src.crm.repositories import CRMRepository

logger = logging.getLogger(__name__)


class ExternalSyncService:
    def __init__(self, repository: CRMRepository, adapters: Dict[str, ExternalCRMAdapter]):
        self.repository = repository
        self.adapters = adapters

    async def sync_deal_from_source(self, source: str, deal_id: str) -> Dict[str, Any]:
        adapter = self.adapters[source]
        rows = await adapter.fetch_deals({"id": deal_id})
        if not rows:
            raise ValueError(f"No deal found in {source} with id {deal_id}")

        row = rows[0]
        internal_deal = await self.repository.get_deal(deal_id)
        if internal_deal is None:
            internal_deal = Deal(
                id=deal_id,
                company_id=row.get("company_id") or "unknown-company",
                owner_id=row.get("owner_id") or "system-owner",
                title=row.get("title") or row.get("name") or "Imported Deal",
                stage=DealStage(row.get("stage", DealStage.LEAD.value)),
                amount=float(row.get("amount", 0.0) or 0.0),
                probability=float(row.get("probability", 0.0) or 0.0),
                source=source,
            )
            await self.repository.save_deal(internal_deal)

        if row.get("amount") and abs(float(row["amount"]) - internal_deal.amount) > 0.01:
            logger.info("Detected amount drift between source and internal system")

        return {
            "deal_id": deal_id,
            "source": source,
            "synced_at": datetime.utcnow().isoformat(),
            "stage": internal_deal.stage.value,
        }
```

---

## KPI and sales pipeline metrics

The CRM layer must provide measurable operational metrics:

- pipeline volume by stage;
- weighted revenue forecast;
- conversion rate by stage;
- average sales cycle length;
- open tasks per rep;
- deal aging and follow-up SLA;
- seller performance by segment;
- win/loss reasons; 
- percent of deals with activity in the last 7 days.

These metrics are not optional; they are the business signal that the sales orchestration layer is executing correctly.

---

## Why Module 3 matters

Module 3 closes the loop between AI decisions and operational sales execution.

It answers the question: after the platform decides the deal is qualified or needs coaching, what actually happens next?

The answer is:
- the deal is assigned,
- the next action is scheduled,
- the activity is logged,
- the stage is advanced or escalated,
- the CRM sync ensures external systems reflect the same state,
- and the pipeline remains consistent across all channels.

Without Module 3, the system would be an analytics engine without execution capability.

This is the layer that converts strategy into measurable commercial action.

---

## Final result

The revenue platform is then structured as:

- Module 1: distributed workflow orchestration;
- Module 2: AI decision layer;
- Module 3: CRM and sales orchestration.

Together they form a complete commercial operating system:

- leads enter the pipeline,
- AI qualifies them,
- workflow coordinates execution,
- CRM records and drives sales activity,
- external systems stay synchronized,
- pipeline health remains auditable and measurable.

This is the complete production foundation for autonomous revenue intelligence at scale.
```
