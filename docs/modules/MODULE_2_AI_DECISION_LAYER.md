# MODULE 2: AI DECISION LAYER
## Полная реализация LLM-агентов для сделок

```python
# src/ai/models.py

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class AgentType(str, Enum):
    LEAD = "lead"
    SALES_COACH = "sales_coach"
    VOICE = "voice"
    FORECAST = "forecast"


class ReasoningMode(str, Enum):
    BASIC = "basic"
    EXTENDED = "extended"
    CHAIN_OF_THOUGHT = "chain_of_thought"


@dataclass
class LeadScoringContext:
    """Контекст для AI-скоринга лида"""
    deal_id: str
    company_name: str
    revenue: float
    industry: str
    location: str
    contact_level: str  # C-suite, director, manager
    budget_indicator: str  # high, medium, low, unknown
    pain_points: List[str]
    company_size: int
    previous_interactions: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_prompt(self) -> str:
        """Преобразование контекста в промпт для LLM"""
        prompt = f"""
Analyze lead qualification based on the following context:

Company: {self.company_name}
Revenue: ${self.revenue:,.0f}
Industry: {self.industry}
Location: {self.location}
Contact Level: {self.contact_level}
Company Size: {self.company_size} employees
Budget Indicator: {self.budget_indicator}

Pain Points:
{chr(10).join(f'- {point}' for point in self.pain_points)}

Previous Interactions: {len(self.previous_interactions)}

Provide a structured qualification decision:
1. Is this lead qualified? (yes/no)
2. Confidence score (0-1)
3. Risk level (low/medium/high)
4. Recommended next step
5. Key objections to prepare for
"""
        return prompt


@dataclass
class ProposalContext:
    """Контекст для генерации КП"""
    deal_id: str
    company_name: str
    contact_name: str
    industry: str
    requirements: str
    budget_range: str
    timeline: str
    decision_criteria: List[str]
    competitor_context: str
    
    def to_prompt(self) -> str:
        """Генерация промпта для создания КП"""
        prompt = f"""
Generate a professional commercial proposal with the following context:

Customer: {self.company_name}
Contact: {self.contact_name}
Industry: {self.industry}
Timeline: {self.timeline}
Budget Range: {self.budget_range}

Requirements:
{self.requirements}

Decision Criteria:
{chr(10).join(f'- {criteria}' for criteria in self.decision_criteria)}

Competitive Context:
{self.competitor_context}

Create a compelling proposal that:
1. Addresses all stated requirements
2. Aligns with their decision criteria
3. Provides clear pricing breakdown
4. Includes implementation timeline
5. Highlights ROI and competitive advantage
6. Addresses potential objections
"""
        return prompt


@dataclass
class SalesCoachContext:
    """Контекст для AI-coach рекомендаций"""
    deal_id: str
    current_stage: str
    days_in_stage: int
    last_interaction: Optional[datetime]
    objections: List[str]
    deal_value: float
    contact_seniority: str
    previous_coaching_actions: List[str]
    
    def to_prompt(self) -> str:
        """Генерация промпта для coach"""
        prompt = f"""
Provide sales coaching recommendation for a deal stuck in negotiation:

Deal Value: ${self.deal_value:,.0f}
Current Stage: {self.current_stage}
Days in Stage: {self.days_in_stage}
Last Interaction: {self.last_interaction.isoformat() if self.last_interaction else 'Unknown'}
Contact Seniority: {self.contact_seniority}

Active Objections:
{chr(10).join(f'- {obj}' for obj in self.objections)}

Previous Coaching Actions Taken:
{chr(10).join(f'- {action}' for action in self.previous_coaching_actions)}

Provide 5-10 specific, actionable recommendations:
1. Next best action (specific and tactical)
2. Key message to emphasize
3. Decision maker to involve
4. Objection handling strategy
5. Timeline for follow-up
6. Risk mitigation approach
7. Win probability assessment
8. Escalation triggers
"""
        return prompt


@dataclass
class AIAgentResponse:
    """Стандартный ответ от AI-агента"""
    agent_type: AgentType
    deal_id: str
    decision: str
    confidence: float
    reasoning: List[str]
    actions: List[str]
    raw_response: str
    model_used: str
    tokens_used: Dict[str, int]
    timestamp: datetime = field(default_factory=datetime.utcnow)
```

```python
# src/ai/llm_gateway.py

import json
import logging
from typing import Dict, Any, Optional
from enum import Enum
import asyncio

import openai
import anthropic

logger = logging.getLogger(__name__)


class LLMProvider(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"


class LLMGateway:
    """Абстракция для работы с различными LLM провайдерами"""
    
    def __init__(
        self,
        primary_provider: LLMProvider = LLMProvider.OPENAI,
        openai_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
        openai_model: str = "gpt-4o",
        anthropic_model: str = "claude-3-5-sonnet-20241022",
        timeout: int = 30,
    ):
        self.primary_provider = primary_provider
        self.openai_model = openai_model
        self.anthropic_model = anthropic_model
        self.timeout = timeout
        
        if openai_api_key:
            openai.api_key = openai_api_key
        if anthropic_api_key:
            self.anthropic_client = anthropic.Anthropic(api_key=anthropic_api_key)
        else:
            self.anthropic_client = anthropic.Anthropic()
    
    async def reason_with_cot(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        """Chain-of-Thought reasoning для сложных решений"""
        
        if self.primary_provider == LLMProvider.OPENAI:
            return await self._openai_cot(
                system_prompt, user_prompt, max_tokens, temperature
            )
        else:
            return await self._anthropic_cot(
                system_prompt, user_prompt, max_tokens, temperature
            )
    
    async def _openai_cot(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> Dict[str, Any]:
        """Chain-of-Thought для OpenAI с расширенным reasoning"""
        
        try:
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: openai.chat.completions.create(
                        model=self.openai_model,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        temperature=temperature,
                        max_tokens=max_tokens,
                        top_p=0.9,
                    ),
                ),
                timeout=self.timeout,
            )
            
            content = response.choices[0].message.content
            
            return {
                "provider": "openai",
                "model": self.openai_model,
                "content": content,
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                },
                "finish_reason": response.choices[0].finish_reason,
            }
        
        except asyncio.TimeoutError:
            logger.error("OpenAI request timeout")
            raise
        except Exception as exc:
            logger.exception("OpenAI error: %s", exc)
            raise
    
    async def _anthropic_cot(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> Dict[str, Any]:
        """Chain-of-Thought для Claude с extended thinking"""
        
        try:
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.anthropic_client.messages.create(
                        model=self.anthropic_model,
                        max_tokens=max_tokens,
                        system=system_prompt,
                        messages=[
                            {"role": "user", "content": user_prompt},
                        ],
                        temperature=temperature,
                    ),
                ),
                timeout=self.timeout,
            )
            
            content = response.content[0].text
            
            return {
                "provider": "anthropic",
                "model": self.anthropic_model,
                "content": content,
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                },
                "stop_reason": response.stop_reason,
            }
        
        except asyncio.TimeoutError:
            logger.error("Anthropic request timeout")
            raise
        except Exception as exc:
            logger.exception("Anthropic error: %s", exc)
            raise
    
    async def extract_json_response(
        self,
        response_text: str,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Парсинг и валидация JSON ответа от LLM"""
        
        try:
            json_start = response_text.find("{")
            json_end = response_text.rfind("}") + 1
            
            if json_start == -1 or json_end == 0:
                logger.error("No JSON found in response")
                raise ValueError("Response does not contain valid JSON")
            
            json_str = response_text[json_start:json_end]
            parsed = json.loads(json_str)
            
            return parsed
        
        except json.JSONDecodeError as exc:
            logger.exception("JSON parse error: %s", exc)
            raise
```

```python
# src/ai/lead_agent.py

import logging
from typing import Dict, Any
from datetime import datetime

from src.ai.models import LeadScoringContext, AIAgentResponse, AgentType
from src.ai.llm_gateway import LLMGateway
from src.orchestration.workflow_engine import DealStatus

logger = logging.getLogger(__name__)


class LeadQualificationAgent:
    """AI-агент для квалификации лидов с ReAct паттерном"""
    
    SYSTEM_PROMPT = """You are an expert lead qualification specialist with deep knowledge of enterprise SaaS sales.

Your task is to evaluate incoming leads based on their firmographic data, engagement signals, and fit with our solution.

Provide your analysis in the following JSON format:
{
    "qualified": true|false,
    "confidence_score": 0.0-1.0,
    "risk_level": "low|medium|high",
    "reasoning": ["reason1", "reason2", ...],
    "next_step": "send_proposal|schedule_call|need_more_info|reject",
    "objections_to_prepare": ["objection1", "objection2", ...],
    "estimated_deal_size": "small|medium|large",
    "action_items": ["item1", "item2", ...]
}

Be conservative with qualification - only qualify if confidence is > 0.65.
Consider industry trends, company maturity, and decision-maker seniority.
"""
    
    def __init__(self, llm_gateway: LLMGateway):
        self.llm = llm_gateway
    
    async def qualify_lead(
        self,
        context: LeadScoringContext,
    ) -> AIAgentResponse:
        """Квалификация лида с использованием Chain-of-Thought reasoning"""
        
        logger.info(f"Qualifying lead {context.deal_id} for {context.company_name}")
        
        user_prompt = context.to_prompt()
        
        llm_response = await self.llm.reason_with_cot(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=1500,
            temperature=0.5,  # Детерминированный скоринг
        )
        
        parsed_response = await self.llm.extract_json_response(
            llm_response["content"],
            schema={},
        )
        
        actions = []
        if parsed_response.get("qualified"):
            actions.append(f"move_to_qualified:deal_id={context.deal_id}")
            actions.append(f"schedule_follow_up:lead_id={context.deal_id}")
        else:
            actions.append(f"mark_rejected:deal_id={context.deal_id}")
        
        # Добавляем action items из LLM
        actions.extend(parsed_response.get("action_items", []))
        
        response = AIAgentResponse(
            agent_type=AgentType.LEAD,
            deal_id=context.deal_id,
            decision="qualified" if parsed_response.get("qualified") else "rejected",
            confidence=parsed_response.get("confidence_score", 0.0),
            reasoning=parsed_response.get("reasoning", []),
            actions=actions,
            raw_response=llm_response["content"],
            model_used=llm_response["model"],
            tokens_used=llm_response["usage"],
        )
        
        logger.info(
            f"Lead {context.deal_id} qualification: {response.decision} "
            f"(confidence={response.confidence:.2f})"
        )
        
        return response
```

```python
# src/ai/proposal_agent.py

import logging
from typing import Dict, Any
from datetime import datetime

from src.ai.models import ProposalContext, AIAgentResponse, AgentType
from src.ai.llm_gateway import LLMGateway

logger = logging.getLogger(__name__)


class ProposalGenerationAgent:
    """AI-агент для генерации коммерческих предложений"""
    
    SYSTEM_PROMPT = """You are an expert commercial proposal writer with 15+ years in enterprise SaaS sales.

Create compelling proposals that:
1. Address all customer pain points explicitly
2. Provide clear ROI justification
3. Include competitive differentiation
4. Offer flexible implementation options
5. Build urgency without pressure

Generate professional, well-structured proposals in HTML format.
Include all required sections: Executive Summary, Solution Overview, Implementation Timeline, Pricing, ROI Analysis, Terms & Conditions.
"""
    
    def __init__(self, llm_gateway: LLMGateway):
        self.llm = llm_gateway
    
    async def generate_proposal(
        self,
        context: ProposalContext,
    ) -> AIAgentResponse:
        """Генерация КП с учётом контекста сделки"""
        
        logger.info(f"Generating proposal for deal {context.deal_id}")
        
        user_prompt = context.to_prompt()
        
        llm_response = await self.llm.reason_with_cot(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=3000,
            temperature=0.7,
        )
        
        proposal_html = llm_response["content"]
        
        actions = [
            f"save_proposal:deal_id={context.deal_id}",
            f"send_proposal:recipient={context.contact_name}@email",
        ]
        
        response = AIAgentResponse(
            agent_type=AgentType.LEAD,
            deal_id=context.deal_id,
            decision="proposal_generated",
            confidence=1.0,
            reasoning=["Proposal generated successfully"],
            actions=actions,
            raw_response=proposal_html,
            model_used=llm_response["model"],
            tokens_used=llm_response["usage"],
        )
        
        logger.info(f"Proposal generated for {context.deal_id}")
        
        return response
```

```python
# src/ai/sales_coach_agent.py

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta

from src.ai.models import SalesCoachContext, AIAgentResponse, AgentType
from src.ai.llm_gateway import LLMGateway
from src.db.models import Deal

logger = logging.getLogger(__name__)


class SalesCoachAgent:
    """AI-агент для советов по продажам (BANT, objection handling)"""
    
    SYSTEM_PROMPT = """You are an expert sales coach with experience in high-ticket enterprise SaaS.

Provide specific, tactical recommendations to move deals forward.
Focus on:
1. Overcoming specific objections
2. Identifying decision makers
3. Creating urgency
4. Building relationships
5. Managing competition

Provide recommendations in JSON format:
{
    "next_action": "specific tactic to execute",
    "key_message": "talking point to emphasize",
    "decision_maker_to_involve": "role and how to reach them",
    "objection_handling": ["objection": "response"] format,
    "timeline": "recommended follow-up frequency",
    "win_probability": 0.0-1.0,
    "risk_indicators": ["risk1", "risk2"],
    "escalation_triggers": ["condition1", "condition2"]
}
"""
    
    def __init__(self, llm_gateway: LLMGateway):
        self.llm = llm_gateway
    
    async def get_coaching_recommendation(
        self,
        context: SalesCoachContext,
        deal_db: Any,
    ) -> AIAgentResponse:
        """Получить рекомендацию coach-а для "застрявшей" сделки"""
        
        logger.info(f"Generating coaching for stuck deal {context.deal_id}")
        
        user_prompt = context.to_prompt()
        
        llm_response = await self.llm.reason_with_cot(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=2000,
            temperature=0.6,
        )
        
        coaching_data = await self.llm.extract_json_response(
            llm_response["content"],
            schema={},
        )
        
        actions = [
            f"log_coaching:deal_id={context.deal_id}",
            f"send_coaching_notification:deal_id={context.deal_id}",
            f"schedule_follow_up:days={coaching_data.get('timeline', '3-5 days')}",
        ]
        
        # Если вероятность выигрыша <20%, добавляем ускоренный follow-up
        win_prob = coaching_data.get("win_probability", 0.5)
        if win_prob < 0.2:
            actions.append(f"escalate_to_manager:deal_id={context.deal_id}")
        
        response = AIAgentResponse(
            agent_type=AgentType.SALES_COACH,
            deal_id=context.deal_id,
            decision=f"coaching_provided:win_prob={win_prob:.1%}",
            confidence=win_prob,
            reasoning=coaching_data.get("risk_indicators", []),
            actions=actions,
            raw_response=llm_response["content"],
            model_used=llm_response["model"],
            tokens_used=llm_response["usage"],
        )
        
        logger.info(
            f"Coaching generated for {context.deal_id}, "
            f"win probability: {win_prob:.1%}"
        )
        
        return response
```

```python
# src/ai/forecast_agent.py

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
import statistics

from src.ai.models import AIAgentResponse, AgentType
from src.ai.llm_gateway import LLMGateway

logger = logging.getLogger(__name__)


class ForecastAgent:
    """AI-агент для прогнозирования выручки и оттока"""
    
    SYSTEM_PROMPT = """You are an expert revenue forecasting specialist.

Analyze the following deal pipeline and provide:
1. Weighted pipeline forecast (using deal probability × deal value)
2. Expected close dates with confidence intervals
3. Churn risk scoring for existing customers
4. Revenue acceleration opportunities
5. Pipeline health assessment

Provide analysis in JSON format:
{
    "forecast_revenue": number,
    "confidence_interval": {"low": number, "high": number},
    "expected_close_dates": ["deal_id": {"date": "YYYY-MM-DD", "probability": 0.0-1.0}],
    "churn_risks": ["customer_id": {"risk_score": 0.0-1.0, "indicators": []}],
    "pipeline_health": "strong|moderate|weak",
    "recommendations": ["rec1", "rec2"]
}
"""
    
    def __init__(self, llm_gateway: LLMGateway):
        self.llm = llm_gateway
    
    async def forecast_revenue(
        self,
        pipeline_deals: List[Dict[str, Any]],
        historical_win_rates: Dict[str, float],
    ) -> AIAgentResponse:
        """Прогнозирование выручки на основе pipeline"""
        
        logger.info(f"Forecasting revenue for {len(pipeline_deals)} deals")
        
        # Подготовка контекста
        user_prompt = self._prepare_forecast_context(
            pipeline_deals,
            historical_win_rates,
        )
        
        llm_response = await self.llm.reason_with_cot(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=2500,
            temperature=0.4,  # Консервативный подход для прогноза
        )
        
        forecast_data = await self.llm.extract_json_response(
            llm_response["content"],
            schema={},
        )
        
        # Вычисляем взвешенный forecast используя deal probabilities
        weighted_revenue = sum(
            deal.get("value", 0) * deal.get("probability", 0)
            for deal in pipeline_deals
        )
        
        response = AIAgentResponse(
            agent_type=AgentType.FORECAST,
            deal_id="pipeline_forecast",
            decision=f"forecast_generated:revenue=${forecast_data.get('forecast_revenue', 0):,.0f}",
            confidence=0.85,
            reasoning=[
                f"Analyzed {len(pipeline_deals)} deals",
                f"Weighted forecast: ${weighted_revenue:,.0f}",
                f"Pipeline health: {forecast_data.get('pipeline_health', 'unknown')}",
            ],
            actions=[
                "publish_forecast:stakeholders=leadership",
                "create_forecast_report:format=pdf",
            ],
            raw_response=llm_response["content"],
            model_used=llm_response["model"],
            tokens_used=llm_response["usage"],
        )
        
        return response
    
    def _prepare_forecast_context(
        self,
        pipeline_deals: List[Dict[str, Any]],
        historical_win_rates: Dict[str, float],
    ) -> str:
        """Подготовка контекста для LLM"""
        
        deals_summary = "\n".join([
            f"- Deal {deal.get('id')}: ${deal.get('value', 0):,.0f}, "
            f"Stage: {deal.get('stage')}, "
            f"Probability: {deal.get('probability', 0):.0%}, "
            f"Days in Stage: {deal.get('days_in_stage', 0)}"
            for deal in pipeline_deals
        ])
        
        return f"""
Analyze the following sales pipeline for revenue forecast:

Pipeline Deals:
{deals_summary}

Historical Win Rates by Stage:
{chr(10).join(f'- {stage}: {rate:.0%}' for stage, rate in historical_win_rates.items())}

Provide detailed forecast with confidence intervals.
"""
```

```python
# src/ai/voice_agent.py

import logging
from typing import Optional, List
from datetime import datetime

from src.ai.models import AIAgentResponse, AgentType
from src.ai.llm_gateway import LLMGateway

logger = logging.getLogger(__name__)


class VoiceAgentController:
    """Контроллер для голосовых агентов (интеграция с WebRTC/STT)"""
    
    SYSTEM_PROMPT = """You are a professional sales development representative.

During calls, you should:
1. Listen actively to customer needs
2. Ask clarifying questions about pain points
3. Introduce solution value proposition
4. Handle objections gracefully
5. Schedule follow-up meetings
6. Build rapport and trust

Respond naturally and conversationally. Keep responses concise (1-2 sentences).
"""
    
    def __init__(self, llm_gateway: LLMGateway):
        self.llm = llm_gateway
        self.call_transcript: List[Dict[str, str]] = []
    
    async def process_voice_input(
        self,
        transcribed_text: str,
        call_id: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Обработка голосовой реплики и генерация ответа"""
        
        # Добавляем в историю разговора
        self.call_transcript.append({
            "speaker": "customer",
            "text": transcribed_text,
            "timestamp": datetime.utcnow().isoformat(),
        })
        
        # Подготавливаем контекст для LLM
        conversation_history = "\n".join([
            f"{turn['speaker']}: {turn['text']}"
            for turn in self.call_transcript[-10:]  # Последние 10 реплик
        ])
        
        user_prompt = f"""
Customer Context:
- Company: {context.get('company_name')}
- Deal Value: ${context.get('deal_value', 0):,.0f}
- Current Stage: {context.get('stage')}

Conversation so far:
{conversation_history}

Customer just said: "{transcribed_text}"

Provide a natural, concise response to continue the conversation.
"""
        
        llm_response = await self.llm.reason_with_cot(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=200,
            temperature=0.7,
        )
        
        agent_response = llm_response["content"].strip()
        
        # Добавляем ответ агента в историю
        self.call_transcript.append({
            "speaker": "agent",
            "text": agent_response,
            "timestamp": datetime.utcnow().isoformat(),
        })
        
        logger.info(f"Voice agent response for call {call_id}: {agent_response[:100]}")
        
        return {
            "response": agent_response,
            "call_id": call_id,
            "timestamp": datetime.utcnow().isoformat(),
            "sentiment_detected": await self._analyze_sentiment(transcribed_text),
        }
    
    async def _analyze_sentiment(self, text: str) -> str:
        """Анализ эмоционального тона"""
        
        sentiment_prompt = f"""
Analyze the sentiment of this customer statement in one word: positive, neutral, or negative.
Statement: "{text}"
Response format: just one word.
"""
        
        response = await self.llm.reason_with_cot(
            system_prompt="You are a sentiment analysis expert. Respond with only one word: positive, neutral, or negative.",
            user_prompt=sentiment_prompt,
            max_tokens=10,
            temperature=0.3,
        )
        
        sentiment = response["content"].strip().lower()
        return sentiment if sentiment in ["positive", "neutral", "negative"] else "neutral"
    
    def get_call_summary(self) -> Dict[str, Any]:
        """Получить краткую сводку разговора"""
        
        if not self.call_transcript:
            return {}
        
        agent_turns = [t for t in self.call_transcript if t["speaker"] == "agent"]
        customer_turns = [t for t in self.call_transcript if t["speaker"] == "customer"]
        
        return {
            "total_turns": len(self.call_transcript),
            "agent_responses": len(agent_turns),
            "customer_messages": len(customer_turns),
            "duration_turns": len(self.call_transcript),
            "full_transcript": self.call_transcript,
        }
```

```python
# src/ai/agent_orchestrator.py

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from src.ai.lead_agent import LeadQualificationAgent
from src.ai.proposal_agent import ProposalGenerationAgent
from src.ai.sales_coach_agent import SalesCoachAgent
from src.ai.forecast_agent import ForecastAgent
from src.ai.voice_agent import VoiceAgentController
from src.ai.llm_gateway import LLMGateway, LLMProvider
from src.ai.models import (
    LeadScoringContext,
    ProposalContext,
    SalesCoachContext,
    AIAgentResponse,
)

logger = logging.getLogger(__name__)


class AIAgentOrchestrator:
    """Главный оркестратор AI-агентов"""
    
    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
    ):
        self.llm = LLMGateway(
            primary_provider=LLMProvider.OPENAI,
            openai_api_key=openai_api_key,
            anthropic_api_key=anthropic_api_key,
        )
        
        self.lead_agent = LeadQualificationAgent(self.llm)
        self.proposal_agent = ProposalGenerationAgent(self.llm)
        self.coach_agent = SalesCoachAgent(self.llm)
        self.forecast_agent = ForecastAgent(self.llm)
        self.voice_agent = VoiceAgentController(self.llm)
    
    async def qualify_lead(self, context: LeadScoringContext) -> AIAgentResponse:
        """Запуск квалификации лида"""
        return await self.lead_agent.qualify_lead(context)
    
    async def generate_proposal(self, context: ProposalContext) -> AIAgentResponse:
        """Запуск генерации КП"""
        return await self.proposal_agent.generate_proposal(context)
    
    async def get_sales_coaching(
        self,
        context: SalesCoachContext,
        deal_db: Any,
    ) -> AIAgentResponse:
        """Получить рекомендацию coach-а"""
        return await self.coach_agent.get_coaching_recommendation(context, deal_db)
    
    async def forecast_revenue(
        self,
        pipeline_deals: list,
        historical_win_rates: Dict[str, float],
    ) -> AIAgentResponse:
        """Прогноз выручки"""
        return await self.forecast_agent.forecast_revenue(
            pipeline_deals,
            historical_win_rates,
        )
    
    async def voice_respond(
        self,
        transcribed_text: str,
        call_id: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Обработка голосовой реплики"""
        return await self.voice_agent.process_voice_input(
            transcribed_text,
            call_id,
            context,
        )
```

---

## Интеграция Module 2 с Module 1

### src/orchestration/workflow_engine.py (дополнение)

Добавляем вызовы AI-агентов в workflow:

```python
# Внутри DealLifecycleWorkflow.execute()

from src.ai.agent_orchestrator import AIAgentOrchestrator
from src.ai.models import LeadScoringContext

# Инициализация AI
ai_orchestrator = AIAgentOrchestrator(
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
)

# ===== STEP 1b: AI Lead Qualification (дополнение к activity) =====
lead_scoring_context = LeadScoringContext(
    deal_id=deal_id,
    company_name=company_data.get("name"),
    revenue=company_data.get("revenue", 0),
    industry=company_data.get("industry", "unknown"),
    location=company_data.get("location", "unknown"),
    contact_level=contact_info.get("level", "manager"),
    budget_indicator=company_data.get("budget_indicator", "unknown"),
    pain_points=company_data.get("pain_points", []),
    company_size=company_data.get("company_size", 0),
    previous_interactions=company_data.get("interactions", []),
)

ai_qualification = await workflow.execute_activity(
    call_ai_lead_agent,
    context=lead_scoring_context,
    start_to_close_timeout=timedelta(seconds=45),
    retry_policy=retry_policy,
)

self._log_event("AI_LEAD_QUALIFICATION", {
    "ai_decision": ai_qualification.decision,
    "ai_confidence": ai_qualification.confidence,
    "ai_reasoning": ai_qualification.reasoning,
})
```

Полная реализация activity:

```python
@activity.defn
async def call_ai_lead_agent(context: LeadScoringContext) -> Dict[str, Any]:
    """Activity для вызова AI lead qualification agent"""
    ai_orchestrator = AIAgentOrchestrator()
    response = await ai_orchestrator.qualify_lead(context)
    return {
        "decision": response.decision,
        "confidence": response.confidence,
        "reasoning": response.reasoning,
        "actions": response.actions,
        "model_used": response.model_used,
        "tokens_used": response.tokens_used,
    }
```

---

## Архитектурные принципы Module 2

1. **LLM Agnosticism**: код работает с OpenAI и Anthropic через единый LLMGateway
2. **Structured Outputs**: все ответы парсятся в JSON и валидируются
3. **Chain-of-Thought Reasoning**: для сложных решений используется расширенное рассуждение
4. **Token Efficiency**: отслеживание использования токенов для оптимизации costs
5. **Async-First**: все LLM-вызовы асинхронные и таймируются
6. **Confidence Scores**: каждое решение сопровождается confidence для risk assessment
7. **Actionable Output**: AI не просто рекомендует, а генерирует конкретные actions для workflow

---

## Performance Targets

| Метрика | Целевое значение |
|---------|----------------|
| Lead qualification latency | < 10 sec |
| Proposal generation | < 30 sec |
| Coach recommendation | < 20 sec |
| Voice response latency | < 2 sec |
| Token usage (qual) | 800-1200 |
| Token usage (proposal) | 2000-3000 |
| Forecast generation | < 15 sec |

