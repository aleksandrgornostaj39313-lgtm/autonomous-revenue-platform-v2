"""
AI Coach Service - LLM-based deal coaching recommendations
2026 Edition: contextual reasoning, confidence scoring, action recommendations
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.core.models import AiCoachRecommendation

logger = logging.getLogger(__name__)


class AiCoachService:
    """
    AI coach service for sales optimization
    
    Uses LLM to analyze deal context and provide next best action.
    """

    def __init__(self, llm_service, deal_history_service, config: Optional[Dict[str, Any]] = None):
        self.llm_service = llm_service
        self.deal_history_service = deal_history_service
        self.config = config or {
            "model": "gpt-4-turbo",
            "temperature": 0.3,
            "max_tokens": 1200,
            "confidence_threshold": 0.7,
        }

    async def get_recommendation(
        self,
        deal_id: str,
        context: Dict[str, Any]
    ) -> AiCoachRecommendation:
        """Get AI recommendation for current deal state"""
        logger.info(f"Getting AI coaching recommendation for deal {deal_id}")

        history = await self.deal_history_service.get_recent_events(deal_id, limit=10)

        prompt = f"""
Ты — опытный B2B-sales AI coach. Проанализируй текущую ситуацию по сделке.

Данные сделки:
- Deal ID: {deal_id}
- Статус: {context.get('status')}
- Без ответа дней: {context.get('days_without_response', 0)}
- Компания: {context.get('company')}

История события:
{history}

Твоя задача:
1. Объясни причину застоя
2. Предложи следующее действие
3. Укажи приоритет (low, medium, high, critical)
4. Дай скользящий рейтинг уверенности 0-1

Верни JSON со структурами:
{{
  "recommendation": "...",
  "urgency": "...",
  "reasoning": "...",
  "suggested_action": "...",
  "confidence_score": 0.0
}}
"""

        raw_response = await self.llm_service.generate(
            prompt=prompt,
            model=self.config["model"],
            temperature=self.config["temperature"],
            max_tokens=self.config["max_tokens"],
        )

        # Parse JSON response if necessary
        parsed = self._parse_json(raw_response)

        return AiCoachRecommendation(
            deal_id=deal_id,
            recommendation=parsed.get("recommendation", "Follow up with decision maker"),
            urgency=parsed.get("urgency", "medium"),
            reasoning=parsed.get("reasoning", "Deal is delayed and requires immediate follow-up"),
            suggested_action=parsed.get("suggested_action", "Schedule direct call with decision maker"),
            confidence_score=float(parsed.get("confidence_score", 0.75)),
            created_at=datetime.utcnow()
        )

    def _parse_json(self, raw_response: str) -> Dict[str, Any]:
        """Parse AI response JSON from text"""
        try:
            import json
            return json.loads(raw_response)
        except Exception:
            # Fallback: parse simplistic text response
            return {
                "recommendation": "Direct follow-up required",
                "urgency": "high",
                "reasoning": raw_response[:500],
                "suggested_action": "Call decision maker and clarify next steps",
                "confidence_score": 0.75
            }
