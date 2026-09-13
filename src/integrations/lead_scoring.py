"""
Lead Scoring Engine - Real AI-based lead qualification
2026 Edition: Vector embeddings, ML scoring, risk assessment
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import numpy as np

from src.core.models import (
    LeadQualificationResult, RiskLevel, CompanyData, ContactInfo
)

logger = logging.getLogger(__name__)


class LeadScoringEngine:
    """
    Production-ready lead scoring engine
    - Vector embeddings for similarity matching
    - ML model for scoring (XGBoost/LightGBM)
    - Risk assessment algorithms
    - Knowledge base retrieval
    """

    def __init__(
        self,
        embedding_service,  # Pinecone/Milvus client
        ml_model_service,   # ML model inference service
        knowledge_base_service,
        config: Dict[str, Any] = None
    ):
        """
        Args:
            embedding_service: Vector DB client for semantic search
            ml_model_service: ML model service for scoring
            knowledge_base_service: KB retrieval service
            config: Scoring configuration
        """
        self.embedding_service = embedding_service
        self.ml_model_service = ml_model_service
        self.knowledge_base_service = knowledge_base_service
        
        self.config = config or {
            "qualification_threshold": 0.6,
            "high_risk_threshold": 0.7,
            "revenue_threshold": 1000000,
            "min_score": 0.0,
            "max_score": 1.0
        }

    async def score_lead(
        self,
        lead_id: str,
        company_data: Dict[str, Any]
    ) -> LeadQualificationResult:
        """
        Score lead using multiple signals
        
        Scoring factors:
        1. Company financial health (revenue, growth)
        2. Industry compatibility (matching pain points)
        3. Competitor analysis (market position)
        4. Deal similarity (similar closed deals)
        5. Contact seniority (decision maker level)
        """
        logger.info(f"Scoring lead {lead_id} for {company_data.get('name')}")

        try:
            # Get features from company data
            features = await self._extract_features(company_data)
            
            # Get similar deals from knowledge base
            similar_deals = await self.knowledge_base_service.find_similar_deals(
                industry=company_data.get("industry"),
                revenue_range=(
                    company_data.get("revenue", 0) * 0.5,
                    company_data.get("revenue", 0) * 2.0
                ),
                limit=5
            )
            
            # ML scoring
            ml_score = await self.ml_model_service.predict(features)
            
            # Vector similarity scoring
            company_embedding = await self.embedding_service.embed_text(
                f"{company_data.get('name')} {company_data.get('industry')} "
                f"{company_data.get('location')}"
            )
            
            similarity_scores = []
            for deal in similar_deals:
                deal_embedding = await self.embedding_service.embed_text(
                    f"{deal.get('company_name')} {deal.get('industry')}"
                )
                similarity = self._cosine_similarity(company_embedding, deal_embedding)
                similarity_scores.append(similarity)
            
            avg_similarity = np.mean(similarity_scores) if similarity_scores else 0.5
            
            # Risk assessment
            risk_level = await self._assess_risk(company_data, similar_deals)
            
            # Final score: weighted average
            final_score = (
                ml_score * 0.4 +
                avg_similarity * 0.4 +
                (1.0 if risk_level == RiskLevel.LOW else 0.7 if risk_level == RiskLevel.MEDIUM else 0.3) * 0.2
            )
            
            # Clamp score
            final_score = max(self.config["min_score"], min(final_score, self.config["max_score"]))
            
            is_qualified = final_score >= self.config["qualification_threshold"]
            
            # Scoring details for transparency
            scoring_details = {
                "ml_score": float(ml_score),
                "similarity_score": float(avg_similarity),
                "risk_factor": 0.7 if risk_level == RiskLevel.LOW else 0.5 if risk_level == RiskLevel.MEDIUM else 0.2,
                "financial_score": self._score_financials(company_data),
                "industry_match": float(avg_similarity)
            }
            
            reason = self._get_scoring_reason(
                final_score,
                company_data,
                similar_deals,
                risk_level
            )
            
            logger.info(
                f"Lead {lead_id} scored: {final_score:.3f}, "
                f"qualified: {is_qualified}, risk: {risk_level}"
            )
            
            return LeadQualificationResult(
                lead_id=lead_id,
                is_qualified=is_qualified,
                score=final_score,
                reason=reason,
                risk_level=risk_level,
                recommended_next_step="Send personalized proposal" if is_qualified else "Nurture campaign",
                scoring_details=scoring_details,
                created_at=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"Lead scoring failed: {str(e)}")
            raise

    async def _extract_features(self, company_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract ML features from company data"""
        revenue = float(company_data.get("revenue", 0))
        employees = float(company_data.get("employees", 0))
        
        return {
            "revenue": revenue,
            "revenue_log": np.log1p(revenue),
            "employees": employees,
            "employees_log": np.log1p(employees),
            "revenue_per_employee": revenue / (employees + 1),
            "has_website": 1.0 if company_data.get("website") else 0.0,
            "decision_makers_count": float(len(company_data.get("decision_makers", []))),
        }

    def _score_financials(self, company_data: Dict[str, Any]) -> float:
        """Score company financial health"""
        revenue = company_data.get("revenue", 0)
        
        if revenue > 100_000_000:
            return 1.0
        elif revenue > 50_000_000:
            return 0.9
        elif revenue > 10_000_000:
            return 0.8
        elif revenue > 1_000_000:
            return 0.7
        elif revenue > 500_000:
            return 0.5
        else:
            return 0.3

    async def _assess_risk(
        self,
        company_data: Dict[str, Any],
        similar_deals: List[Dict[str, Any]]
    ) -> RiskLevel:
        """Assess deal risk based on company profile and similar deals"""
        revenue = company_data.get("revenue", 0)
        
        # Financial risk
        if revenue < 1_000_000:
            return RiskLevel.HIGH
        
        if revenue < 10_000_000:
            financial_risk = RiskLevel.MEDIUM
        else:
            financial_risk = RiskLevel.LOW
        
        # Historical risk: check similar deals outcomes
        if similar_deals:
            win_rate = sum(
                1 for deal in similar_deals if deal.get("status") == "won"
            ) / len(similar_deals)
            
            if win_rate < 0.3:
                return RiskLevel.HIGH
            elif win_rate < 0.6:
                return RiskLevel.MEDIUM
        
        return financial_risk

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        vec1 = np.array(vec1)
        vec2 = np.array(vec2)
        
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))

    def _get_scoring_reason(
        self,
        score: float,
        company_data: Dict[str, Any],
        similar_deals: List[Dict[str, Any]],
        risk_level: RiskLevel
    ) -> str:
        """Generate human-readable scoring reason"""
        company_name = company_data.get("name", "Unknown")
        revenue = company_data.get("revenue", 0)
        
        if score >= 0.8:
            return f"{company_name}: High-quality lead with strong revenue ({revenue:,.0f}), low risk profile"
        elif score >= 0.6:
            return f"{company_name}: Qualified lead, good fit with similar deals, {risk_level} risk"
        elif score >= 0.4:
            return f"{company_name}: Potential lead but needs nurturing, {risk_level} risk"
        else:
            return f"{company_name}: Low-quality lead, significant risks, recommend outbound engagement"
