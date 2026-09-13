"""
Proposal Generation Service - Real LLM-based proposal creation
2026 Edition: LiteLLM with GPT-4, Claude, multi-provider support
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json

import aiohttp
from src.core.models import (
    ProposalGenerationResult, ProposalLine, CompanyData, ContactInfo
)

logger = logging.getLogger(__name__)


class ProposalGeneratorService:
    """
    Production-ready proposal generator
    - LLM-based content generation (GPT-4, Claude 3.5)
    - Knowledge base retrieval for context
    - Price calculation based on market data
    - Template customization
    - PDF/Word export support
    """

    def __init__(
        self,
        llm_service,  # LiteLLM client
        knowledge_base_service,
        pricing_engine,
        template_engine,
        config: Dict[str, Any] = None
    ):
        """
        Args:
            llm_service: LLM inference service (OpenAI, Anthropic, etc.)
            knowledge_base_service: KB for context retrieval
            pricing_engine: Dynamic pricing calculator
            template_engine: Proposal template processor
            config: Generation configuration
        """
        self.llm_service = llm_service
        self.knowledge_base_service = knowledge_base_service
        self.pricing_engine = pricing_engine
        self.template_engine = template_engine
        
        self.config = config or {
            "model": "gpt-4-turbo",
            "temperature": 0.7,
            "max_tokens": 2000,
            "top_p": 0.9,
            "validity_days": 30,
            "include_testimonials": True,
            "include_case_studies": True,
            "language": "ru"
        }

    async def generate_proposal(
        self,
        deal_id: str,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str
    ) -> ProposalGenerationResult:
        """
        Generate personalized proposal using LLM
        
        Process:
        1. Retrieve similar successful proposals from KB
        2. Extract company context (industry, size, challenges)
        3. Generate proposal using LLM with few-shot examples
        4. Calculate pricing based on market data
        5. Create proposal lines with details
        6. Format final output
        """
        logger.info(f"Generating proposal for deal {deal_id}, company: {company_data.get('name')}")

        try:
            # Step 1: Get context from knowledge base
            kb_context = await self.knowledge_base_service.retrieve_context(
                query=f"{requirements} for {company_data.get('industry')}",
                limit=3,
                min_relevance=0.7
            )
            
            # Step 2: Get similar case studies
            case_studies = await self.knowledge_base_service.find_case_studies(
                industry=company_data.get("industry"),
                company_size=company_data.get("employees"),
                limit=2
            )
            
            # Step 3: Calculate pricing
            pricing_data = await self.pricing_engine.calculate_pricing(
                requirements=requirements,
                company_revenue=company_data.get("revenue", 0),
                industry=company_data.get("industry")
            )
            
            # Step 4: Build LLM prompt with context
            prompt = self._build_proposal_prompt(
                company_data=company_data,
                contact_info=contact_info,
                requirements=requirements,
                kb_context=kb_context,
                case_studies=case_studies,
                pricing_data=pricing_data
            )
            
            # Step 5: Generate proposal content via LLM
            proposal_content = await self.llm_service.generate(
                prompt=prompt,
                model=self.config["model"],
                temperature=self.config["temperature"],
                max_tokens=self.config["max_tokens"],
                top_p=self.config["top_p"]
            )
            
            # Step 6: Generate proposal lines
            proposal_lines = await self._generate_proposal_lines(
                pricing_data=pricing_data,
                requirements=requirements
            )
            
            # Step 7: Calculate total value
            total_value = sum(line.total for line in proposal_lines)
            
            # Step 8: Create proposal object
            proposal = ProposalGenerationResult(
                deal_id=deal_id,
                content=proposal_content,
                lines=proposal_lines,
                estimated_value=total_value,
                validity_days=self.config["validity_days"],
                valid_until=datetime.utcnow() + timedelta(days=self.config["validity_days"]),
                generated_by="LLM_GENERATION_ENGINE",
                metadata={
                    "model": self.config["model"],
                    "kb_sources": [ctx.get("id") for ctx in kb_context],
                    "case_studies": [cs.get("id") for cs in case_studies],
                    "language": self.config["language"],
                    "company_name": company_data.get("name"),
                    "industry": company_data.get("industry")
                }
            )
            
            logger.info(
                f"Proposal {proposal.proposal_id} generated for {deal_id}, "
                f"value: {total_value:,.0f} {pricing_data.get('currency', 'RUB')}"
            )
            
            return proposal
            
        except Exception as e:
            logger.error(f"Proposal generation failed: {str(e)}")
            raise

    def _build_proposal_prompt(
        self,
        company_data: Dict[str, Any],
        contact_info: Dict[str, Any],
        requirements: str,
        kb_context: List[Dict[str, Any]],
        case_studies: List[Dict[str, Any]],
        pricing_data: Dict[str, Any]
    ) -> str:
        """Build detailed LLM prompt for proposal generation"""
        
        context_text = "\n".join([
            f"- {ctx.get('title')}: {ctx.get('summary')}"
            for ctx in kb_context
        ])
        
        case_study_text = "\n".join([
            f"- {cs.get('company')}: Achieved {cs.get('result')} with {cs.get('solution')}"
            for cs in case_studies
        ])
        
        prompt = f"""
Ты опытный B2B менеджер. Создай профессиональное коммерческое предложение.

КОМПАНИЯ-КЛИЕНТ:
- Название: {company_data.get('name')}
- Индустрия: {company_data.get('industry')}
- Размер: {company_data.get('employees')} сотрудников
- Выручка: {company_data.get('revenue'):,.0f} руб.
- Локация: {company_data.get('location')}

КОНТАКТНОЕ ЛИЦО:
- Имя: {contact_info.get('name')}
- Должность: {contact_info.get('title')}
- Email: {contact_info.get('email')}

ТРЕБОВАНИЯ КЛИЕНТА:
{requirements}

РЕЛЕВАНТНЫЙ КОНТЕНТ ИЗ БАЗЫ ЗНАНИЙ:
{context_text}

УСПЕШНЫЕ КЕЙСЫ:
{case_study_text}

ЦЕНООБРАЗОВАНИЕ:
- Консультация: {pricing_data.get('consultation_price', 150000):,.0f} руб.
- Реализация: {pricing_data.get('implementation_price', 250000):,.0f} руб.
- Обучение: {pricing_data.get('training_price', 50000):,.0f} руб.

ТРЕБОВАНИЯ К ВЫВОДУ:
1. Профессиональное приветствие
2. Анализ требований и боли клиента (2-3 абзаца)
3. Предложенное решение с преимуществами (3-4 абзаца)
4. Детальный план реализации (этапы, сроки)
5. Описание команды и опыта
6. Сметы и условия оплаты
7. Гарантии и поддержка
8. Call to action

Язык: Русский
Тон: Профессиональный, доступный
Объём: 800-1200 слов
"""
        
        return prompt

    async def _generate_proposal_lines(
        self,
        pricing_data: Dict[str, Any],
        requirements: str
    ) -> List[ProposalLine]:
        """Generate proposal line items"""
        
        lines = []
        
        # Consultation line
        consultation_qty = pricing_data.get("consultation_days", 5)
        consultation_price = pricing_data.get("consultation_price", 30000)
        
        lines.append(ProposalLine(
            description="Консультация и анализ текущих процессов",
            quantity=consultation_qty,
            unit_price=consultation_price / consultation_qty
        ))
        lines[-1].calculate_total()
        
        # Implementation line
        implementation_price = pricing_data.get("implementation_price", 250000)
        lines.append(ProposalLine(
            description="Реализация решения и кастомизация",
            quantity=1,
            unit_price=implementation_price
        ))
        lines[-1].calculate_total()
        
        # Training line
        training_days = pricing_data.get("training_days", 3)
        training_price = pricing_data.get("training_price", 50000)
        
        lines.append(ProposalLine(
            description="Обучение команды",
            quantity=training_days,
            unit_price=training_price / training_days
        ))
        lines[-1].calculate_total()
        
        # Support line
        support_price = pricing_data.get("support_price", 30000)
        lines.append(ProposalLine(
            description="Техническая поддержка (3 месяца)",
            quantity=1,
            unit_price=support_price
        ))
        lines[-1].calculate_total()
        
        return lines

    async def save_proposal_as_pdf(
        self,
        proposal: ProposalGenerationResult,
        output_path: str
    ) -> str:
        """
        Export proposal to PDF
        
        Uses: reportlab or WeasyPrint
        """
        try:
            from reportlab.lib.pagesizes import letter, A4
            from reportlab.lib import colors
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            
            logger.info(f"Exporting proposal {proposal.proposal_id} to PDF")
            
            # Create PDF
            doc = SimpleDocTemplate(
                output_path,
                pagesize=A4,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=18
            )
            
            # Content
            story = []
            styles = getSampleStyleSheet()
            
            # Title
            story.append(Paragraph(
                f"Коммерческое предложение №{proposal.proposal_id}",
                styles['Heading1']
            ))
            story.append(Spacer(1, 12))
            
            # Date
            story.append(Paragraph(
                f"Дата создания: {proposal.generated_at.strftime('%d.%m.%Y')}",
                styles['Normal']
            ))
            story.append(Spacer(1, 12))
            
            # Content
            story.append(Paragraph(proposal.content, styles['Normal']))
            story.append(Spacer(1, 12))
            
            # Table of line items
            table_data = [
                ["Описание", "Кол-во", "Цена", "Итого"]
            ]
            
            for line in proposal.lines:
                table_data.append([
                    line.description,
                    f"{line.quantity:.0f}",
                    f"{line.unit_price:,.0f}",
                    f"{line.total:,.0f}"
                ])
            
            # Add total row
            table_data.append([
                "ИТОГО",
                "",
                "",
                f"{proposal.estimated_value:,.0f}"
            ])
            
            table = Table(table_data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, -1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(table)
            story.append(Spacer(1, 12))
            
            # Validity
            story.append(Paragraph(
                f"Предложение действительно до {proposal.valid_until.strftime('%d.%m.%Y')}",
                styles['Normal']
            ))
            
            # Build PDF
            doc.build(story)
            
            logger.info(f"PDF exported to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"PDF export failed: {str(e)}")
            raise
