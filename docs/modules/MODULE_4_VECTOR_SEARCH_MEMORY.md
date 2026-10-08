# MODULE 4: ADVANCED VECTOR SEARCH & MEMORY
## Semantic retrieval, knowledge graphs, deal context, and persistent memory for AI reasoning

```python
# src/vector_store/models.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class EmbeddingModel(str, Enum):
    OPENAI_SMALL = "text-embedding-3-small"
    OPENAI_LARGE = "text-embedding-3-large"
    ANTHROPIC = "claude-3-5-sonnet-20241022"


class VectorDocumentType(str, Enum):
    DEAL_SUMMARY = "deal_summary"
    CALL_TRANSCRIPT = "call_transcript"
    EMAIL = "email"
    PROPOSAL = "proposal"
    COMPANY_PROFILE = "company_profile"
    CONVERSATION_TURN = "conversation_turn"
    OBJECTION = "objection"
    NEGOTIATION_NOTE = "negotiation_note"
    CASE_LIBRARY = "case_library"
    PLAYBOOK = "playbook"


@dataclass
class VectorDocument:
    """Document to be embedded and stored in vector DB"""
    id: str
    deal_id: str
    document_type: VectorDocumentType
    content: str
    metadata: Dict[str, Any]
    embedding: Optional[List[float]] = None
    embedding_model: str = EmbeddingModel.OPENAI_SMALL.value
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def summary(self) -> str:
        """Return content for embedding"""
        return self.content


@dataclass
class VectorSearchResult:
    """Result from vector search"""
    document_id: str
    deal_id: str
    document_type: VectorDocumentType
    content: str
    similarity_score: float
    metadata: Dict[str, Any]


@dataclass
class DealMemory:
    """Persistent memory context for a deal"""
    deal_id: str
    company_name: str
    contact_names: List[str]
    key_pain_points: List[str]
    proposed_solution: Optional[str] = None
    objections_raised: List[str] = field(default_factory=list)
    win_factors: List[str] = field(default_factory=list)
    loss_factors: List[str] = field(default_factory=list)
    timeline_notes: Optional[str] = None
    budget_range: Optional[str] = None
    decision_criteria: List[str] = field(default_factory=list)
    competitor_intelligence: Optional[str] = None
    relationship_history: List[Dict[str, Any]] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CompanyKnowledgeNode:
    """Node in company knowledge graph"""
    node_id: str
    node_type: str  # "company", "person", "product", "market", "competitor"
    entity_id: str
    label: str
    properties: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class KnowledgeGraphEdge:
    """Edge connecting two knowledge nodes"""
    edge_id: str
    source_node_id: str
    target_node_id: str
    relationship: str  # "is_competitor", "uses_product", "located_in", etc.
    metadata: Dict[str, Any] = field(default_factory=dict)
    strength: float = 1.0  # 0-1, confidence/weight
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SemanticSearchQuery:
    """Structured query for semantic search"""
    query_text: str
    deal_id: Optional[str] = None
    document_types: Optional[List[VectorDocumentType]] = None
    top_k: int = 5
    similarity_threshold: float = 0.6
    filters: Dict[str, Any] = field(default_factory=dict)
```

```python
# src/vector_store/embedding_service.py

from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import openai

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating embeddings using OpenAI"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "text-embedding-3-small"):
        if api_key:
            openai.api_key = api_key
        self.model = model
        self.cache: dict[str, List[float]] = {}
    
    async def embed_text(self, text: str) -> List[float]:
        """Generate embedding for text"""
        text_normalized = text.strip()
        
        if text_normalized in self.cache:
            logger.debug(f"Returning cached embedding for text: {text_normalized[:50]}")
            return self.cache[text_normalized]
        
        try:
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: openai.embeddings.create(
                        model=self.model,
                        input=text_normalized,
                    ),
                ),
                timeout=30,
            )
            
            embedding = response.data[0].embedding
            self.cache[text_normalized] = embedding
            
            logger.info(f"Generated embedding (model={self.model}, dims={len(embedding)})")
            return embedding
        
        except asyncio.TimeoutError:
            logger.error(f"Embedding request timeout for text: {text_normalized[:50]}")
            raise
        except Exception as exc:
            logger.exception(f"Embedding error: {exc}")
            raise
    
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts"""
        results = []
        for text in texts:
            embedding = await self.embed_text(text)
            results.append(embedding)
        return results


class EmbeddingCacheService:
    """Caching layer for embeddings"""
    
    def __init__(self, redis_client: Optional[object] = None):
        self.redis = redis_client
        self.local_cache: dict[str, List[float]] = {}
    
    async def get_or_generate(
        self,
        text: str,
        embedding_service: EmbeddingService,
    ) -> List[float]:
        """Get cached embedding or generate new one"""
        
        cache_key = f"embedding:{hash(text)}"
        
        if self.redis:
            try:
                cached = await self.redis.get(cache_key)
                if cached:
                    logger.debug("Retrieved embedding from Redis")
                    return cached
            except Exception as exc:
                logger.warning(f"Redis retrieval failed: {exc}")
        
        if text in self.local_cache:
            logger.debug("Retrieved embedding from local cache")
            return self.local_cache[text]
        
        embedding = await embedding_service.embed_text(text)
        self.local_cache[text] = embedding
        
        if self.redis:
            try:
                await self.redis.set(cache_key, embedding, ex=86400)  # 24 hour TTL
            except Exception as exc:
                logger.warning(f"Redis set failed: {exc}")
        
        return embedding
```

```python
# src/vector_store/pinecone_backend.py

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    import pinecone
except ImportError:
    pinecone = None

logger = logging.getLogger(__name__)


class PineconeVectorStore:
    """Pinecone-backed vector database"""
    
    def __init__(
        self,
        api_key: str,
        environment: str,
        index_name: str,
        namespace: str = "default",
        dimension: int = 1536,
    ):
        if pinecone is None:
            raise ImportError("pinecone library not installed")
        
        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.namespace = namespace
        self.dimension = dimension
        
        pinecone.init(api_key=api_key, environment=environment)
        self.index = pinecone.Index(index_name)
    
    async def upsert_vectors(
        self,
        vectors: List[tuple[str, List[float], Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Insert or update vectors in Pinecone"""
        
        try:
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.index.upsert(
                        vectors=[(v[0], v[1], v[2]) for v in vectors],
                        namespace=self.namespace,
                    ),
                ),
                timeout=30,
            )
            
            logger.info(f"Upserted {len(vectors)} vectors to Pinecone")
            return {"upserted_count": len(vectors), "status": "success"}
        
        except Exception as exc:
            logger.exception(f"Pinecone upsert failed: {exc}")
            raise
    
    async def query_vectors(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[tuple[str, float, Dict[str, Any]]]:
        """Query vectors from Pinecone"""
        
        try:
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.index.query(
                        vector=query_embedding,
                        top_k=top_k,
                        include_metadata=True,
                        filter=filters,
                        namespace=self.namespace,
                    ),
                ),
                timeout=30,
            )
            
            results = [
                (match["id"], match["score"], match.get("metadata", {}))
                for match in response["matches"]
            ]
            
            logger.info(f"Retrieved {len(results)} results from Pinecone")
            return results
        
        except Exception as exc:
            logger.exception(f"Pinecone query failed: {exc}")
            raise
    
    async def delete_vectors(self, vector_ids: List[str]) -> Dict[str, Any]:
        """Delete vectors from Pinecone"""
        
        try:
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.index.delete(
                        ids=vector_ids,
                        namespace=self.namespace,
                    ),
                ),
                timeout=30,
            )
            
            logger.info(f"Deleted {len(vector_ids)} vectors from Pinecone")
            return {"deleted_count": len(vector_ids), "status": "success"}
        
        except Exception as exc:
            logger.exception(f"Pinecone delete failed: {exc}")
            raise


import asyncio
```

```python
# src/vector_store/repositories.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.vector_store.models import (
    VectorDocument,
    VectorSearchResult,
    SemanticSearchQuery,
    DealMemory,
    CompanyKnowledgeNode,
    KnowledgeGraphEdge,
)


class VectorDocumentRepository(ABC):
    @abstractmethod
    async def save_document(self, doc: VectorDocument) -> VectorDocument:
        raise NotImplementedError

    @abstractmethod
    async def get_document(self, doc_id: str) -> Optional[VectorDocument]:
        raise NotImplementedError

    @abstractmethod
    async def list_documents_by_deal(self, deal_id: str) -> List[VectorDocument]:
        raise NotImplementedError

    @abstractmethod
    async def delete_document(self, doc_id: str) -> bool:
        raise NotImplementedError


class PostgresVectorDocumentRepository(VectorDocumentRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def save_document(self, doc: VectorDocument) -> VectorDocument:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO vector_documents (
                    id, deal_id, document_type, content, metadata, embedding,
                    embedding_model, created_at, updated_at
                ) VALUES (
                    :id, :deal_id, :document_type, :content, :metadata, :embedding,
                    :embedding_model, :created_at, :updated_at
                )
                ON CONFLICT (id) DO UPDATE SET
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    embedding = EXCLUDED.embedding,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "id": doc.id,
                    "deal_id": doc.deal_id,
                    "document_type": doc.document_type.value,
                    "content": doc.content,
                    "metadata": doc.metadata,
                    "embedding": doc.embedding,
                    "embedding_model": doc.embedding_model,
                    "created_at": doc.created_at,
                    "updated_at": doc.updated_at,
                },
            )
            await session.commit()
        return doc

    async def get_document(self, doc_id: str) -> Optional[VectorDocument]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                "SELECT * FROM vector_documents WHERE id = :doc_id",
                {"doc_id": doc_id},
            )
        if row is None:
            return None
        return VectorDocument(
            id=row["id"],
            deal_id=row["deal_id"],
            document_type=row["document_type"],
            content=row["content"],
            metadata=row["metadata"],
            embedding=row["embedding"],
            embedding_model=row["embedding_model"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def list_documents_by_deal(self, deal_id: str) -> List[VectorDocument]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                """
                SELECT * FROM vector_documents 
                WHERE deal_id = :deal_id 
                ORDER BY created_at DESC
                """,
                {"deal_id": deal_id},
            )
        return [
            VectorDocument(
                id=row["id"],
                deal_id=row["deal_id"],
                document_type=row["document_type"],
                content=row["content"],
                metadata=row["metadata"],
                embedding=row["embedding"],
                embedding_model=row["embedding_model"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def delete_document(self, doc_id: str) -> bool:
        async with self.session_factory() as session:
            result = await session.execute(
                "DELETE FROM vector_documents WHERE id = :doc_id",
                {"doc_id": doc_id},
            )
            await session.commit()
        return result.rowcount > 0


class DealMemoryRepository(ABC):
    @abstractmethod
    async def save_memory(self, memory: DealMemory) -> DealMemory:
        raise NotImplementedError

    @abstractmethod
    async def get_memory(self, deal_id: str) -> Optional[DealMemory]:
        raise NotImplementedError


class PostgresDealMemoryRepository(DealMemoryRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def save_memory(self, memory: DealMemory) -> DealMemory:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO deal_memory (
                    deal_id, company_name, contact_names, key_pain_points,
                    proposed_solution, objections_raised, win_factors, loss_factors,
                    timeline_notes, budget_range, decision_criteria,
                    competitor_intelligence, relationship_history, last_updated
                ) VALUES (
                    :deal_id, :company_name, :contact_names, :key_pain_points,
                    :proposed_solution, :objections_raised, :win_factors, :loss_factors,
                    :timeline_notes, :budget_range, :decision_criteria,
                    :competitor_intelligence, :relationship_history, :last_updated
                )
                ON CONFLICT (deal_id) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    contact_names = EXCLUDED.contact_names,
                    key_pain_points = EXCLUDED.key_pain_points,
                    proposed_solution = EXCLUDED.proposed_solution,
                    objections_raised = EXCLUDED.objections_raised,
                    win_factors = EXCLUDED.win_factors,
                    loss_factors = EXCLUDED.loss_factors,
                    timeline_notes = EXCLUDED.timeline_notes,
                    budget_range = EXCLUDED.budget_range,
                    decision_criteria = EXCLUDED.decision_criteria,
                    competitor_intelligence = EXCLUDED.competitor_intelligence,
                    relationship_history = EXCLUDED.relationship_history,
                    last_updated = EXCLUDED.last_updated
                """,
                {
                    "deal_id": memory.deal_id,
                    "company_name": memory.company_name,
                    "contact_names": memory.contact_names,
                    "key_pain_points": memory.key_pain_points,
                    "proposed_solution": memory.proposed_solution,
                    "objections_raised": memory.objections_raised,
                    "win_factors": memory.win_factors,
                    "loss_factors": memory.loss_factors,
                    "timeline_notes": memory.timeline_notes,
                    "budget_range": memory.budget_range,
                    "decision_criteria": memory.decision_criteria,
                    "competitor_intelligence": memory.competitor_intelligence,
                    "relationship_history": memory.relationship_history,
                    "last_updated": memory.last_updated,
                },
            )
            await session.commit()
        return memory

    async def get_memory(self, deal_id: str) -> Optional[DealMemory]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                "SELECT * FROM deal_memory WHERE deal_id = :deal_id",
                {"deal_id": deal_id},
            )
        if row is None:
            return None
        return DealMemory(
            deal_id=row["deal_id"],
            company_name=row["company_name"],
            contact_names=row["contact_names"],
            key_pain_points=row["key_pain_points"],
            proposed_solution=row["proposed_solution"],
            objections_raised=row["objections_raised"],
            win_factors=row["win_factors"],
            loss_factors=row["loss_factors"],
            timeline_notes=row["timeline_notes"],
            budget_range=row["budget_range"],
            decision_criteria=row["decision_criteria"],
            competitor_intelligence=row["competitor_intelligence"],
            relationship_history=row["relationship_history"],
            last_updated=row["last_updated"],
        )


class KnowledgeGraphRepository(ABC):
    @abstractmethod
    async def save_node(self, node: CompanyKnowledgeNode) -> CompanyKnowledgeNode:
        raise NotImplementedError

    @abstractmethod
    async def save_edge(self, edge: KnowledgeGraphEdge) -> KnowledgeGraphEdge:
        raise NotImplementedError

    @abstractmethod
    async def get_node(self, node_id: str) -> Optional[CompanyKnowledgeNode]:
        raise NotImplementedError

    @abstractmethod
    async def get_edges_from_node(self, source_node_id: str) -> List[KnowledgeGraphEdge]:
        raise NotImplementedError

    @abstractmethod
    async def query_neighborhood(self, node_id: str, hops: int = 2) -> Dict[str, Any]:
        raise NotImplementedError


class PostgresKnowledgeGraphRepository(KnowledgeGraphRepository):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory

    async def save_node(self, node: CompanyKnowledgeNode) -> CompanyKnowledgeNode:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO knowledge_nodes (
                    node_id, node_type, entity_id, label, properties, created_at
                ) VALUES (
                    :node_id, :node_type, :entity_id, :label, :properties, :created_at
                )
                ON CONFLICT (node_id) DO UPDATE SET
                    label = EXCLUDED.label,
                    properties = EXCLUDED.properties
                """,
                {
                    "node_id": node.node_id,
                    "node_type": node.node_type,
                    "entity_id": node.entity_id,
                    "label": node.label,
                    "properties": node.properties,
                    "created_at": node.created_at,
                },
            )
            await session.commit()
        return node

    async def save_edge(self, edge: KnowledgeGraphEdge) -> KnowledgeGraphEdge:
        async with self.session_factory() as session:
            await session.execute(
                """
                INSERT INTO knowledge_edges (
                    edge_id, source_node_id, target_node_id, relationship,
                    metadata, strength, created_at
                ) VALUES (
                    :edge_id, :source_node_id, :target_node_id, :relationship,
                    :metadata, :strength, :created_at
                )
                ON CONFLICT (edge_id) DO UPDATE SET
                    relationship = EXCLUDED.relationship,
                    metadata = EXCLUDED.metadata,
                    strength = EXCLUDED.strength
                """,
                {
                    "edge_id": edge.edge_id,
                    "source_node_id": edge.source_node_id,
                    "target_node_id": edge.target_node_id,
                    "relationship": edge.relationship,
                    "metadata": edge.metadata,
                    "strength": edge.strength,
                    "created_at": edge.created_at,
                },
            )
            await session.commit()
        return edge

    async def get_node(self, node_id: str) -> Optional[CompanyKnowledgeNode]:
        async with self.session_factory() as session:
            row = await session.fetch_one(
                "SELECT * FROM knowledge_nodes WHERE node_id = :node_id",
                {"node_id": node_id},
            )
        if row is None:
            return None
        return CompanyKnowledgeNode(
            node_id=row["node_id"],
            node_type=row["node_type"],
            entity_id=row["entity_id"],
            label=row["label"],
            properties=row["properties"],
            created_at=row["created_at"],
        )

    async def get_edges_from_node(self, source_node_id: str) -> List[KnowledgeGraphEdge]:
        async with self.session_factory() as session:
            rows = await session.fetch_all(
                "SELECT * FROM knowledge_edges WHERE source_node_id = :source_node_id",
                {"source_node_id": source_node_id},
            )
        return [
            KnowledgeGraphEdge(
                edge_id=row["edge_id"],
                source_node_id=row["source_node_id"],
                target_node_id=row["target_node_id"],
                relationship=row["relationship"],
                metadata=row["metadata"],
                strength=row["strength"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def query_neighborhood(self, node_id: str, hops: int = 2) -> Dict[str, Any]:
        """Query the k-hop neighborhood of a node"""
        async with self.session_factory() as session:
            # Simplified recursive query for neighborhood
            query = f"""
            WITH RECURSIVE neighborhood AS (
                SELECT node_id, node_type, entity_id, label, properties, 0 as hop
                FROM knowledge_nodes
                WHERE node_id = :node_id
                
                UNION ALL
                
                SELECT kn.node_id, kn.node_type, kn.entity_id, kn.label, kn.properties, n.hop + 1
                FROM knowledge_nodes kn
                INNER JOIN knowledge_edges ke ON kn.node_id = ke.target_node_id
                INNER JOIN neighborhood n ON ke.source_node_id = n.node_id
                WHERE n.hop < :hops
            )
            SELECT * FROM neighborhood
            """
            rows = await session.fetch_all(query, {"node_id": node_id, "hops": hops})
        
        nodes_by_hop = {}
        for row in rows:
            hop = row["hop"]
            if hop not in nodes_by_hop:
                nodes_by_hop[hop] = []
            nodes_by_hop[hop].append({
                "node_id": row["node_id"],
                "node_type": row["node_type"],
                "label": row["label"],
                "properties": row["properties"],
            })
        
        return {
            "center_node_id": node_id,
            "neighborhood": nodes_by_hop,
            "total_nodes": sum(len(v) for v in nodes_by_hop.values()),
        }
```

```python
# src/vector_store/semantic_search_service.py

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from src.vector_store.embedding_service import EmbeddingService
from src.vector_store.models import (
    SemanticSearchQuery,
    VectorDocument,
    VectorSearchResult,
    VectorDocumentType,
)
from src.vector_store.pinecone_backend import PineconeVectorStore
from src.vector_store.repositories import (
    VectorDocumentRepository,
    DealMemoryRepository,
)

logger = logging.getLogger(__name__)


class SemanticSearchService:
    """Service for semantic search across all deal documents"""
    
    def __init__(
        self,
        vector_store: PineconeVectorStore,
        doc_repository: VectorDocumentRepository,
        embedding_service: EmbeddingService,
    ):
        self.vector_store = vector_store
        self.doc_repository = doc_repository
        self.embedding_service = embedding_service
    
    async def search(
        self,
        query: SemanticSearchQuery,
    ) -> List[VectorSearchResult]:
        """Perform semantic search"""
        
        logger.info(f"Semantic search: '{query.query_text}' (deal={query.deal_id})")
        
        query_embedding = await self.embedding_service.embed_text(query.query_text)
        
        filters = {}
        if query.deal_id:
            filters["deal_id"] = {"$eq": query.deal_id}
        if query.document_types:
            filters["document_type"] = {"$in": [dt.value for dt in query.document_types]}
        
        results = await self.vector_store.query_vectors(
            query_embedding=query_embedding,
            top_k=query.top_k,
            filters=filters if filters else None,
        )
        
        search_results = []
        for doc_id, similarity_score, metadata in results:
            if similarity_score < query.similarity_threshold:
                continue
            
            doc = await self.doc_repository.get_document(doc_id)
            if doc is None:
                logger.warning(f"Document {doc_id} not found in repository")
                continue
            
            search_results.append(
                VectorSearchResult(
                    document_id=doc_id,
                    deal_id=doc.deal_id,
                    document_type=doc.document_type,
                    content=doc.content,
                    similarity_score=similarity_score,
                    metadata=metadata,
                )
            )
        
        logger.info(f"Search returned {len(search_results)} results")
        return search_results
    
    async def index_document(self, document: VectorDocument) -> VectorDocument:
        """Index a document for semantic search"""
        
        logger.info(f"Indexing document: {document.id} (type={document.document_type.value})")
        
        # Generate embedding
        document.embedding = await self.embedding_service.embed_text(document.summary())
        
        # Save to repository
        saved_doc = await self.doc_repository.save_document(document)
        
        # Upsert to vector store
        vector_data = [
            (
                saved_doc.id,
                saved_doc.embedding,
                {
                    "deal_id": saved_doc.deal_id,
                    "document_type": saved_doc.document_type.value,
                    "created_at": saved_doc.created_at.isoformat(),
                    **saved_doc.metadata,
                },
            )
        ]
        
        await self.vector_store.upsert_vectors(vector_data)
        
        logger.info(f"Document {document.id} indexed successfully")
        return saved_doc
    
    async def search_similar_cases(
        self,
        deal_id: str,
        case_type: str = "sales",
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Search for similar cases from case library"""
        
        query = SemanticSearchQuery(
            query_text=case_type,
            document_types=[VectorDocumentType.CASE_LIBRARY],
            top_k=top_k,
            similarity_threshold=0.7,
        )
        
        results = await self.search(query)
        
        return [
            {
                "case_id": result.document_id,
                "similarity": result.similarity_score,
                "content_summary": result.content[:200],
                "metadata": result.metadata,
            }
            for result in results
        ]
```

```python
# src/vector_store/deal_context_service.py

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.vector_store.models import (
    DealMemory,
    VectorDocument,
    VectorDocumentType,
    SemanticSearchQuery,
)
from src.vector_store.repositories import (
    DealMemoryRepository,
    VectorDocumentRepository,
)
from src.vector_store.semantic_search_service import SemanticSearchService

logger = logging.getLogger(__name__)


class DealContextService:
    """Service for maintaining and querying deal context and memory"""
    
    def __init__(
        self,
        memory_repository: DealMemoryRepository,
        doc_repository: VectorDocumentRepository,
        search_service: SemanticSearchService,
    ):
        self.memory_repository = memory_repository
        self.doc_repository = doc_repository
        self.search_service = search_service
    
    async def initialize_deal_memory(
        self,
        deal_id: str,
        company_name: str,
        contact_names: List[str],
        pain_points: List[str],
    ) -> DealMemory:
        """Initialize memory for a new deal"""
        
        memory = DealMemory(
            deal_id=deal_id,
            company_name=company_name,
            contact_names=contact_names,
            key_pain_points=pain_points,
        )
        
        saved = await self.memory_repository.save_memory(memory)
        logger.info(f"Initialized memory for deal {deal_id}")
        return saved
    
    async def record_objection(
        self,
        deal_id: str,
        objection: str,
    ) -> DealMemory:
        """Record an objection raised during negotiation"""
        
        memory = await self.memory_repository.get_memory(deal_id)
        if memory is None:
            raise ValueError(f"Deal {deal_id} memory not found")
        
        if objection not in memory.objections_raised:
            memory.objections_raised.append(objection)
        
        memory.last_updated = datetime.utcnow()
        updated = await self.memory_repository.save_memory(memory)
        
        # Index objection as vector document for future reference
        doc = VectorDocument(
            id=f"objection-{deal_id}-{len(memory.objections_raised)}",
            deal_id=deal_id,
            document_type=VectorDocumentType.OBJECTION,
            content=objection,
            metadata={"objection_index": len(memory.objections_raised)},
        )
        await self.search_service.index_document(doc)
        
        logger.info(f"Recorded objection for deal {deal_id}: {objection[:50]}")
        return updated
    
    async def record_proposal(
        self,
        deal_id: str,
        proposal_content: str,
    ) -> DealMemory:
        """Record the proposed solution"""
        
        memory = await self.memory_repository.get_memory(deal_id)
        if memory is None:
            raise ValueError(f"Deal {deal_id} memory not found")
        
        memory.proposed_solution = proposal_content
        memory.last_updated = datetime.utcnow()
        updated = await self.memory_repository.save_memory(memory)
        
        # Index proposal as vector document
        doc = VectorDocument(
            id=f"proposal-{deal_id}",
            deal_id=deal_id,
            document_type=VectorDocumentType.PROPOSAL,
            content=proposal_content,
            metadata={"proposal_version": 1},
        )
        await self.search_service.index_document(doc)
        
        logger.info(f"Recorded proposal for deal {deal_id}")
        return updated
    
    async def get_deal_context_for_ai(
        self,
        deal_id: str,
    ) -> Dict[str, Any]:
        """Get comprehensive deal context for AI reasoning"""
        
        memory = await self.memory_repository.get_memory(deal_id)
        if memory is None:
            raise ValueError(f"Deal {deal_id} memory not found")
        
        # Retrieve all documents related to deal
        documents = await self.doc_repository.list_documents_by_deal(deal_id)
        
        # Organize by type
        docs_by_type = {}
        for doc in documents:
            doc_type = doc.document_type.value
            if doc_type not in docs_by_type:
                docs_by_type[doc_type] = []
            docs_by_type[doc_type].append(doc.content[:500])  # Truncate for context
        
        context = {
            "deal_id": deal_id,
            "company_name": memory.company_name,
            "contacts": memory.contact_names,
            "pain_points": memory.key_pain_points,
            "proposed_solution": memory.proposed_solution,
            "objections": memory.objections_raised,
            "budget": memory.budget_range,
            "timeline": memory.timeline_notes,
            "decision_criteria": memory.decision_criteria,
            "competitor_intelligence": memory.competitor_intelligence,
            "documents_by_type": docs_by_type,
            "last_updated": memory.last_updated.isoformat(),
        }
        
        logger.info(f"Retrieved context for deal {deal_id}")
        return context
    
    async def retrieve_objection_strategies(
        self,
        deal_id: str,
        objection: str,
    ) -> List[Dict[str, Any]]:
        """Retrieve similar objections and strategies from case library"""
        
        query = SemanticSearchQuery(
            query_text=objection,
            document_types=[VectorDocumentType.OBJECTION, VectorDocumentType.PLAYBOOK],
            top_k=10,
            similarity_threshold=0.65,
        )
        
        results = await self.search_service.search(query)
        
        strategies = []
        for result in results:
            strategies.append({
                "similar_objection": result.content,
                "similarity_score": result.similarity_score,
                "source_deal": result.deal_id,
                "document_type": result.document_type.value,
            })
        
        return strategies
```

```python
# src/vector_store/knowledge_graph_service.py

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List

from src.vector_store.models import (
    CompanyKnowledgeNode,
    KnowledgeGraphEdge,
)
from src.vector_store.repositories import KnowledgeGraphRepository

logger = logging.getLogger(__name__)


class KnowledgeGraphService:
    """Service for building and querying company knowledge graphs"""
    
    def __init__(self, repository: KnowledgeGraphRepository):
        self.repository = repository
    
    async def add_company_node(
        self,
        entity_id: str,
        company_name: str,
        properties: Dict[str, Any],
    ) -> CompanyKnowledgeNode:
        """Add a company node to the knowledge graph"""
        
        node = CompanyKnowledgeNode(
            node_id=f"company-{entity_id}",
            node_type="company",
            entity_id=entity_id,
            label=company_name,
            properties=properties,
        )
        
        saved = await self.repository.save_node(node)
        logger.info(f"Added company node: {company_name}")
        return saved
    
    async def add_person_node(
        self,
        entity_id: str,
        name: str,
        role: str,
        properties: Dict[str, Any],
    ) -> CompanyKnowledgeNode:
        """Add a person node to the knowledge graph"""
        
        node = CompanyKnowledgeNode(
            node_id=f"person-{entity_id}",
            node_type="person",
            entity_id=entity_id,
            label=name,
            properties={**properties, "role": role},
        )
        
        saved = await self.repository.save_node(node)
        logger.info(f"Added person node: {name} ({role})")
        return saved
    
    async def add_product_node(
        self,
        entity_id: str,
        product_name: str,
        properties: Dict[str, Any],
    ) -> CompanyKnowledgeNode:
        """Add a product node to the knowledge graph"""
        
        node = CompanyKnowledgeNode(
            node_id=f"product-{entity_id}",
            node_type="product",
            entity_id=entity_id,
            label=product_name,
            properties=properties,
        )
        
        saved = await self.repository.save_node(node)
        logger.info(f"Added product node: {product_name}")
        return saved
    
    async def connect_nodes(
        self,
        source_node_id: str,
        target_node_id: str,
        relationship: str,
        strength: float = 1.0,
        metadata: Dict[str, Any] = None,
    ) -> KnowledgeGraphEdge:
        """Create an edge between two nodes"""
        
        edge = KnowledgeGraphEdge(
            edge_id=str(uuid.uuid4()),
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            relationship=relationship,
            strength=strength,
            metadata=metadata or {},
        )
        
        saved = await self.repository.save_edge(edge)
        logger.info(
            f"Connected {source_node_id} --[{relationship}]--> {target_node_id}"
        )
        return saved
    
    async def build_company_graph(
        self,
        company_id: str,
        company_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build a knowledge graph for a company"""
        
        # Add company node
        company_node = await self.add_company_node(
            entity_id=company_id,
            company_name=company_data["name"],
            properties={
                "industry": company_data.get("industry"),
                "region": company_data.get("region"),
                "size": company_data.get("employee_count"),
            },
        )
        
        # Add contact nodes and connect them
        for contact in company_data.get("contacts", []):
            person_node = await self.add_person_node(
                entity_id=contact["id"],
                name=contact["name"],
                role=contact.get("role", "Unknown"),
                properties={"email": contact.get("email")},
            )
            
            # Connect person to company
            await self.connect_nodes(
                source_node_id=person_node.node_id,
                target_node_id=company_node.node_id,
                relationship="works_at",
                strength=1.0,
            )
        
        # Add competitor nodes if available
        for competitor in company_data.get("competitors", []):
            competitor_node = await self.add_company_node(
                entity_id=competitor["id"],
                company_name=competitor["name"],
                properties={"industry": competitor.get("industry")},
            )
            
            await self.connect_nodes(
                source_node_id=company_node.node_id,
                target_node_id=competitor_node.node_id,
                relationship="competes_with",
                strength=0.8,
            )
        
        # Query neighborhood
        neighborhood = await self.repository.query_neighborhood(
            node_id=company_node.node_id,
            hops=2,
        )
        
        logger.info(f"Built knowledge graph for company {company_id}")
        return {
            "company_node_id": company_node.node_id,
            "neighborhood": neighborhood,
        }
    
    async def get_decision_influencers(
        self,
        company_node_id: str,
    ) -> List[Dict[str, Any]]:
        """Get potential decision influencers in a company"""
        
        edges = await self.repository.get_edges_from_node(company_node_id)
        
        influencers = []
        for edge in edges:
            if edge.relationship == "works_at":
                target_node = await self.repository.get_node(edge.target_node_id)
                if target_node and target_node.node_type == "person":
                    influencers.append({
                        "person_id": target_node.entity_id,
                        "name": target_node.label,
                        "role": target_node.properties.get("role"),
                        "influence_score": edge.strength,
                    })
        
        # Sort by influence
        influencers.sort(key=lambda x: x["influence_score"], reverse=True)
        return influencers
```

```python
# src/vector_store/service_layer.py

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from src.vector_store.deal_context_service import DealContextService
from src.vector_store.embedding_service import EmbeddingService, EmbeddingCacheService
from src.vector_store.knowledge_graph_service import KnowledgeGraphService
from src.vector_store.models import (
    DealMemory,
    SemanticSearchQuery,
    VectorDocument,
    VectorDocumentType,
)
from src.vector_store.pinecone_backend import PineconeVectorStore
from src.vector_store.repositories import (
    DealMemoryRepository,
    KnowledgeGraphRepository,
    VectorDocumentRepository,
)
from src.vector_store.semantic_search_service import SemanticSearchService

logger = logging.getLogger(__name__)


class VectorStoreService:
    """High-level orchestrator for vector store, memory, and knowledge operations"""
    
    def __init__(
        self,
        vector_store: PineconeVectorStore,
        doc_repository: VectorDocumentRepository,
        memory_repository: DealMemoryRepository,
        knowledge_graph_repository: KnowledgeGraphRepository,
        embedding_service: EmbeddingService,
        redis_client: Optional[object] = None,
    ):
        self.vector_store = vector_store
        self.doc_repository = doc_repository
        self.memory_repository = memory_repository
        self.knowledge_graph_repository = knowledge_graph_repository
        
        # Initialize sub-services
        self.embedding_cache = EmbeddingCacheService(redis_client)
        self.search_service = SemanticSearchService(
            vector_store=vector_store,
            doc_repository=doc_repository,
            embedding_service=embedding_service,
        )
        self.context_service = DealContextService(
            memory_repository=memory_repository,
            doc_repository=doc_repository,
            search_service=self.search_service,
        )
        self.knowledge_service = KnowledgeGraphService(
            repository=knowledge_graph_repository,
        )
    
    async def index_call_transcript(
        self,
        deal_id: str,
        transcript: str,
        call_metadata: Dict[str, Any],
    ) -> VectorDocument:
        """Index a call transcript for semantic search"""
        
        doc = VectorDocument(
            id=f"transcript-{deal_id}-{call_metadata.get('call_id', 'unknown')}",
            deal_id=deal_id,
            document_type=VectorDocumentType.CALL_TRANSCRIPT,
            content=transcript,
            metadata=call_metadata,
        )
        
        return await self.search_service.index_document(doc)
    
    async def index_email(
        self,
        deal_id: str,
        email_subject: str,
        email_body: str,
        from_contact: str,
    ) -> VectorDocument:
        """Index an email for semantic search"""
        
        combined_content = f"Subject: {email_subject}\n\n{email_body}"
        
        doc = VectorDocument(
            id=f"email-{deal_id}-{hash(combined_content) % 10000}",
            deal_id=deal_id,
            document_type=VectorDocumentType.EMAIL,
            content=combined_content,
            metadata={
                "subject": email_subject,
                "from": from_contact,
            },
        )
        
        return await self.search_service.index_document(doc)
    
    async def semantic_search(
        self,
        query_text: str,
        deal_id: Optional[str] = None,
        document_types: Optional[List[VectorDocumentType]] = None,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Perform semantic search across deal documents"""
        
        query = SemanticSearchQuery(
            query_text=query_text,
            deal_id=deal_id,
            document_types=document_types,
            top_k=top_k,
            similarity_threshold=0.6,
        )
        
        results = await self.search_service.search(query)
        
        return [
            {
                "document_id": r.document_id,
                "deal_id": r.deal_id,
                "type": r.document_type.value,
                "content": r.content[:300],
                "similarity": r.similarity_score,
            }
            for r in results
        ]
    
    async def get_deal_ai_context(self, deal_id: str) -> Dict[str, Any]:
        """Get comprehensive context for AI agent reasoning"""
        
        return await self.context_service.get_deal_context_for_ai(deal_id)
    
    async def record_deal_activity(
        self,
        deal_id: str,
        activity_type: VectorDocumentType,
        content: str,
        metadata: Dict[str, Any],
    ) -> VectorDocument:
        """Record any deal activity (call, email, note, etc.)"""
        
        doc = VectorDocument(
            id=f"{activity_type.value}-{deal_id}-{hash(content) % 10000}",
            deal_id=deal_id,
            document_type=activity_type,
            content=content,
            metadata=metadata,
        )
        
        return await self.search_service.index_document(doc)
```

---

## Integration with Module 1, 2, 3

### From orchestration workflow (Module 1):

```python
# In src/orchestration/workflow_engine.py

from src.vector_store.service_layer import VectorStoreService
from src.vector_store.models import VectorDocumentType

# During workflow execution
vector_store_service = VectorStoreService(...)

# After proposal generation (Module 2)
await vector_store_service.record_deal_activity(
    deal_id=deal_id,
    activity_type=VectorDocumentType.PROPOSAL,
    content=proposal_content,
    metadata={"proposal_id": proposal.proposal_id},
)

# After call transcript (Module 6)
await vector_store_service.index_call_transcript(
    deal_id=deal_id,
    transcript=call_transcript,
    call_metadata={"call_id": call_id, "duration": duration},
)
```

### From AI agents (Module 2):

```python
# In src/ai/agent_orchestrator.py

async def get_sales_coaching(
    self,
    context: SalesCoachContext,
    deal_db: Any,
    vector_store_service: VectorStoreService,
) -> AIAgentResponse:
    # Retrieve deal context from vector store
    deal_context = await vector_store_service.get_deal_ai_context(context.deal_id)
    
    # Use context to enrich coaching prompt
    enhanced_prompt = f"""
    Deal: {deal_context['company_name']}
    Objections raised: {deal_context['objections']}
    Timeline: {deal_context['timeline']}
    
    {original_coaching_prompt}
    """
    
    # Continue with LLM call...
    response = await self.llm.reason_with_cot(...)
    return response
```

### From CRM (Module 3):

```python
# In src/crm/service_layer.py

async def create_deal(
    self,
    payload: Dict[str, Any],
    vector_store_service: VectorStoreService,
) -> Deal:
    deal = Deal(...)
    created = await self.repository.save_deal(deal)
    
    # Initialize deal memory and knowledge graph
    await vector_store_service.context_service.initialize_deal_memory(
        deal_id=created.id,
        company_name=payload["company_name"],
        contact_names=payload.get("contacts", []),
        pain_points=payload.get("pain_points", []),
    )
    
    # Build knowledge graph for company
    await vector_store_service.knowledge_service.build_company_graph(
        company_id=payload["company_id"],
        company_data=payload.get("company_data", {}),
    )
    
    return created
```

---

## Architecture summary: Module 4

Vector Store & Memory is the **intelligence backbone** of the platform:

1. **Semantic Search** finds relevant knowledge at decision time
2. **Deal Memory** preserves context across workflow steps
3. **Knowledge Graphs** model company structure and relationships
4. **Case Library** provides proven winning strategies and objection responses
5. **Multi-modal Indexing** handles transcripts, emails, proposals, notes

This layer bridges AI decision-making with operational CRM state and provides the retrieval-augmented generation (RAG) foundation that makes the platform stateful and context-aware rather than request-response only.

Without Module 4, each AI call would be stateless; with it, the platform builds institutional memory about each deal and can reason over entire deal history as context.

---

## Performance targets for Module 4

| Metric | Target |
|--------|--------|
| Embedding generation (cached) | < 50ms |
| Vector similarity search | < 200ms |
| Deal context retrieval | < 300ms |
| Knowledge graph k-hop query | < 500ms |
| Batch indexing (100 docs) | < 5s |
| Cache hit rate | > 80% |
| Search recall@5 | > 85% |

