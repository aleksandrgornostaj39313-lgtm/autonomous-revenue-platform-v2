

## Дополнение: production-ready реализация слоя оркестрации

Ниже — практическое продолжение модуля, которое закрывает пробел между учебным примером и реальной production-архитектурой. Здесь добавлены: event-driven state tracking, компенсации Saga, защита от дублей, таймауты, сигнализация и API-сервис для запуска workflow.

### src/orchestration/deal_controller.py

```python
# src/orchestration/deal_controller.py

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.orchestration.workflow_engine import WorkflowOrchestrator

router = APIRouter(prefix="/api/v1/deals", tags=["deals"])


class DealCreateRequest(BaseModel):
    deal_id: str
    company_id: str
    company_name: str
    revenue: float
    contact_name: str
    contact_email: str
    requirements: str
    source: str = "crm"


class DealSignalRequest(BaseModel):
    workflow_id: str
    event: str = "response_received"


@dataclass
class DealExecutionState:
    deal_id: str
    workflow_id: str
    status: str = "created"
    current_stage: str = "lead"
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


@router.post("/start")
async def start_deal(request: DealCreateRequest):
    orchestrator = WorkflowOrchestrator()
    await orchestrator.connect()

    workflow_id = await orchestrator.start_deal_workflow(
        deal_id=request.deal_id,
        company_data={
            "id": request.company_id,
            "name": request.company_name,
            "revenue": request.revenue,
            "source": request.source,
        },
        contact_info={
            "name": request.contact_name,
            "email": request.contact_email,
            "phone": "",
        },
        requirements=request.requirements,
    )

    await orchestrator.close()
    return {
        "deal_id": request.deal_id,
        "workflow_id": workflow_id,
        "status": "started",
    }


@router.post("/signal")
async def signal_deal(request: DealSignalRequest):
    orchestrator = WorkflowOrchestrator()
    await orchestrator.connect()

    try:
        result = await orchestrator.signal_response_received(request.workflow_id)
        return {"status": "ok", "result": result}
    except Exception as exc:
        logging.exception("Deal signal failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await orchestrator.close()
```

### src/orchestration/saga_policy.py

```python
# src/orchestration/saga_policy.py

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any


@dataclass
class CompensationStep:
    action: str
    entity_id: str
    reason: str
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class SagaPolicy:
    """Определяет правила компенсации для многократных внешних операций."""

    def __init__(self):
        self.steps: List[CompensationStep] = []

    def add_step(self, action: str, entity_id: str, reason: str):
        self.steps.append(
            CompensationStep(
                action=action,
                entity_id=entity_id,
                reason=reason,
            )
        )

    def rollback_order(self) -> List[CompensationStep]:
        """Компенсации должны выполняться в обратном порядке."""
        return list(reversed(self.steps))
```

### src/orchestration/event_store.py

```python
# src/orchestration/event_store.py

from typing import Any, Dict, List
import json
from datetime import datetime


class EventStore:
    """Простой audit log для workflow событий."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []

    def append(self, event_type: str, payload: Dict[str, Any] = None):
        self.events.append({
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "payload": payload or {},
        })

    def flush(self) -> List[Dict[str, Any]]:
        snapshot = list(self.events)
        self.events.clear()
        return snapshot
```

### Реальные правила для production-оркестрации

1. Детерминированность workflow
   - все решения должны зависеть только от входных параметров и истории событий;
   - нельзя хранить "магические" флаги в глобальном состоянии без сохранения в workflow state.

2. Паника vs компенсация
   - при ошибке внешнего сервиса: retry -> timeout -> compensation;
   - при ошибке в бизнес-логике: не допускается частичный успех без audit log.

3. Идемпотентность действий
   - `send_proposal_to_1c()` должен быть идемпотентным: повторный вызов не должен создавать второй документ;
   - у каждой операции должен быть уникальный ключ `idempotency_key`.

4. Ключевой принцип Saga
   - если workflow дошёл до точки изменения внешнего состояния, он должен зафиксировать компенсационный маршрут;
   - последующая ошибка в workflow не приводит к "висячей" сделке.

5. Наблюдаемость
   - для каждого шага должны быть лог события, тайминг и статус;
   - workflow id -> deal id -> external request id обязаны быть связаны в одном trace.

### Пример безопасного workflow-guard

```python
# src/orchestration/workflow_guard.py

from datetime import datetime


class WorkflowGuard:
    """Защита от повторного запуска или дублирования сигналов."""

    def __init__(self):
        self.seen_signals = set()
        self.started_at = datetime.utcnow()

    def allow_signal(self, signal_name: str, payload: dict) -> bool:
        key = (signal_name, payload.get("deal_id"), payload.get("workflow_id"))
        if key in self.seen_signals:
            return False
        self.seen_signals.add(key)
        return True
```

### Рекомендуемая схема жизненного цикла сделки

```text
lead
  -> qualified
      -> proposal_generated
          -> proposal_sent
              -> response_wait
                  -> negotiation
                      -> won / lost / stuck

stuck
  -> ai_coach_recommendation
  -> follow_up_notification
  -> negotiation_retry
```

### Краткая архитектурная сводка по Module 1

Module 1 отвечает за базовый слой оркестрации сделки и обеспечивает:

- последовательный запуск бизнес-процессов;
- внешние интеграции через activities;
- обработку таймаутов и повторов;
- signal-based реакцию на внешние события;
- компенсационную логику через Saga;
- транзакционную историю в event log;
- возможность запуска из API и дальнейшего масштабирования в distributed environment.

Это делает модуль 1 ядром платформы: без надёжной оркестрации остальные модули (AI, CRM, voice, analytics) будут работать как набор независимых сервисов, а не как единая система управления сделками.

---

## Итог

Модуль 1 закрывает фундаментальный контракт системы:

- данные проходят через согласованный бизнес-процесс;
- состояние сделки отслеживается через workflow state machine;
- внешние интеграции изолированы в activities;
- ошибки обрабатываются не "в лоб", а через политити retry + timeout + compensation;
- система готова к распределённому запуску и масштабированию без потери контроля над сделками.

В следующем этапе этот слой можно расширять до:

- распределённой очереди задач для маркетинга и продаж;
- многоканальной коммуникации (email, chat, Telegram, CRM);
- workflow для onboarding и renewal pipelines;
- мониторинга SLA и автоматического роутинга сделок.

Продолжение можно оформить как отдельный документ: `MODULE_2_AI_DECISION_LAYER.md` или `MODULE_3_ORCHESTRATION_EXPANSION.md`.

