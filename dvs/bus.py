"""Byte accounting bus for the distributed vector search simulator.

Every message size follows proposal/proposal.tex Section 4 strictly:
- Vector: 384 floats * 4 bytes = 1,536 bytes
- Summary: 16 centers * 1,536 bytes = 24,576 bytes
- Search result list: 10 * (8-byte ID + 4-byte score) = 120 bytes
- Shard search (query out + result back): 1,536 + 120 = 1,656 bytes
- Per-shard sample cutoffs: 64 * (4-byte m-th shard score + 4-byte 10th result score) = 512 bytes
- Per-shard refresh score reply: 4 bytes
- Sample-search replies: 64 * m * (10 * 4-byte score) = 64 * m * 40 bytes
- Setup sample vectors: 64 * 1,536 = 98,304 bytes per shard (reported both as setup and spread over epochs)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

DIM: int = 384
FLOAT_BYTES: int = 4
ID_BYTES: int = 8
TOP_K: int = 10
SUMMARY_CENTERS: int = 16
SAMPLE_QUERIES_COUNT: int = 64

VECTOR_BYTES: int = DIM * FLOAT_BYTES  # 1,536
SUMMARY_BYTES: int = SUMMARY_CENTERS * VECTOR_BYTES  # 24,576
RESULT_ITEM_BYTES: int = ID_BYTES + FLOAT_BYTES  # 12
RESULT_BYTES: int = TOP_K * RESULT_ITEM_BYTES  # 120
SHARD_SEARCH_BYTES: int = VECTOR_BYTES + RESULT_BYTES  # 1,656

CUTOFFS_PER_SHARD_BYTES: int = SAMPLE_QUERIES_COUNT * (FLOAT_BYTES + FLOAT_BYTES)  # 512
SHARD_SCORE_BYTES: int = FLOAT_BYTES  # 4
SAMPLE_REPLY_PER_SHARD_QUERY_BYTES: int = TOP_K * FLOAT_BYTES  # 40
SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES: int = SAMPLE_QUERIES_COUNT * VECTOR_BYTES  # 98,304

BUDGETED_CATEGORIES = frozenset({
    "summary_refresh",
    "rn_sample_search_reply",
    "rn_cutoffs",
    "rn_shard_score",
    "extra_search_query",
    "extra_search_result",
    "push_vector",
    "setup_sample_vectors_amortized",
})

WORKLOAD_CATEGORIES = frozenset({
    "query_out",
    "query_result",
})

SETUP_CATEGORIES = frozenset({
    "setup_sample_vectors",
    "setup_initial_summary",
})


def recovered_neighbor_overhead_bytes(num_shards: int, m: int, num_sample_queries: int = SAMPLE_QUERIES_COUNT) -> int:
    """Return per-epoch overhead in bytes for the recovered_neighbor refresh rule.

    At S=32, m=2, Q_s=64:
      512 * 32 (16,384) + 4 * 32 (128) + 64 * 2 * 40 (5,120) = 21,632 bytes.
    """
    cutoffs = num_sample_queries * (FLOAT_BYTES + FLOAT_BYTES) * num_shards
    scores = SHARD_SCORE_BYTES * num_shards
    sample_replies = num_sample_queries * m * SAMPLE_REPLY_PER_SHARD_QUERY_BYTES
    return cutoffs + scores + sample_replies


@dataclass(frozen=True)
class MessageRecord:
    """Single logged message on the simulated network bus."""

    epoch: int
    category: str
    bytes: int
    src: str
    dst: str
    charged_to_budget: bool
    note: str = ""


class ByteBus:
    """Logs all simulated network messages and computes per-epoch, per-category byte totals."""

    def __init__(self, num_shards: int, num_epochs: int = 50) -> None:
        self.num_shards = int(num_shards)
        self.num_epochs = int(num_epochs)
        self.messages: List[MessageRecord] = []
        self._setup_sample_vectors_bytes: int = 0

    def log(
        self,
        epoch: int,
        category: str,
        num_bytes: int,
        src: str = "coordinator",
        dst: str = "shard",
        charged_to_budget: Optional[bool] = None,
        note: str = "",
    ) -> MessageRecord:
        if num_bytes < 0:
            raise ValueError(f"Message byte size must be non-negative, got {num_bytes}")
        if charged_to_budget is None:
            charged_to_budget = category in BUDGETED_CATEGORIES
        rec = MessageRecord(
            epoch=int(epoch),
            category=category,
            bytes=int(num_bytes),
            src=src,
            dst=dst,
            charged_to_budget=bool(charged_to_budget),
            note=note,
        )
        self.messages.append(rec)
        if category == "setup_sample_vectors":
            self._setup_sample_vectors_bytes += int(num_bytes)
        return rec

    def log_setup_initial_summaries(self, num_shards: int, c: int = SUMMARY_CENTERS) -> None:
        summary_size = c * VECTOR_BYTES
        for s in range(num_shards):
            self.log(
                epoch=-1,
                category="setup_initial_summary",
                num_bytes=summary_size,
                src=f"shard_{s}",
                dst="coordinator",
                charged_to_budget=False,
            )

    def log_setup_sample_vectors(
        self,
        num_shards: int,
        num_sample_queries: int = SAMPLE_QUERIES_COUNT,
    ) -> int:
        per_shard = num_sample_queries * VECTOR_BYTES
        total = 0
        for s in range(num_shards):
            self.log(
                epoch=-1,
                category="setup_sample_vectors",
                num_bytes=per_shard,
                src="coordinator",
                dst=f"shard_{s}",
                charged_to_budget=False,
            )
            total += per_shard
        return total

    def log_query_search(self, epoch: int, shard_id: int, query_idx: int) -> int:
        self.log(
            epoch=epoch,
            category="query_out",
            num_bytes=VECTOR_BYTES,
            src="coordinator",
            dst=f"shard_{shard_id}",
            charged_to_budget=False,
            note=f"q={query_idx}",
        )
        self.log(
            epoch=epoch,
            category="query_result",
            num_bytes=RESULT_BYTES,
            src=f"shard_{shard_id}",
            dst="coordinator",
            charged_to_budget=False,
            note=f"q={query_idx}",
        )
        return SHARD_SEARCH_BYTES

    def log_extra_search(self, epoch: int, shard_id: int, query_idx: int) -> int:
        self.log(
            epoch=epoch,
            category="extra_search_query",
            num_bytes=VECTOR_BYTES,
            src="coordinator",
            dst=f"shard_{shard_id}",
            charged_to_budget=True,
            note=f"q={query_idx}",
        )
        self.log(
            epoch=epoch,
            category="extra_search_result",
            num_bytes=RESULT_BYTES,
            src=f"shard_{shard_id}",
            dst="coordinator",
            charged_to_budget=True,
            note=f"q={query_idx}",
        )
        return SHARD_SEARCH_BYTES

    def log_push_vector(self, epoch: int, shard_id: int, doc_id: int) -> int:
        self.log(
            epoch=epoch,
            category="push_vector",
            num_bytes=VECTOR_BYTES,
            src=f"shard_{shard_id}",
            dst="coordinator",
            charged_to_budget=True,
            note=f"doc={doc_id}",
        )
        return VECTOR_BYTES

    def log_summary_refresh(self, epoch: int, shard_id: int, c: int = SUMMARY_CENTERS) -> int:
        size = c * VECTOR_BYTES
        self.log(
            epoch=epoch,
            category="summary_refresh",
            num_bytes=size,
            src=f"shard_{shard_id}",
            dst="coordinator",
            charged_to_budget=True,
        )
        return size

    def log_recovered_neighbor_overhead(
        self,
        epoch: int,
        num_shards: int,
        m: int,
        num_sample_queries: int = SAMPLE_QUERIES_COUNT,
    ) -> int:
        # 1. Sample query search replies from top-m shards for each sample query: 64 * m * 40 bytes
        reply_bytes_per_query_shard = SAMPLE_REPLY_PER_SHARD_QUERY_BYTES
        for q_idx in range(num_sample_queries):
            for rank in range(m):
                self.log(
                    epoch=epoch,
                    category="rn_sample_search_reply",
                    num_bytes=reply_bytes_per_query_shard,
                    src=f"top_shard_rank_{rank}",
                    dst="coordinator",
                    charged_to_budget=True,
                    note=f"sample_q={q_idx}",
                )
        # 2. Cutoffs sent to each shard: 512 bytes per shard
        cutoff_bytes = num_sample_queries * (FLOAT_BYTES + FLOAT_BYTES)
        for s in range(num_shards):
            self.log(
                epoch=epoch,
                category="rn_cutoffs",
                num_bytes=cutoff_bytes,
                src="coordinator",
                dst=f"shard_{s}",
                charged_to_budget=True,
            )
        # 3. Scalar refresh score returned by each shard: 4 bytes per shard
        for s in range(num_shards):
            self.log(
                epoch=epoch,
                category="rn_shard_score",
                num_bytes=SHARD_SCORE_BYTES,
                src=f"shard_{s}",
                dst="coordinator",
                charged_to_budget=True,
            )
        return recovered_neighbor_overhead_bytes(num_shards, m, num_sample_queries)

    def epoch_messages(self, epoch: int) -> List[MessageRecord]:
        return [m for m in self.messages if m.epoch == epoch]

    def epoch_category_totals(self, epoch: int) -> Dict[str, int]:
        totals: Dict[str, int] = {
            "summary_refresh": 0,
            "rn_sample_search_reply": 0,
            "rn_cutoffs": 0,
            "rn_shard_score": 0,
            "extra_search_query": 0,
            "extra_search_result": 0,
            "push_vector": 0,
            "setup_sample_vectors_amortized": 0,
            "query_out": 0,
            "query_result": 0,
        }
        for msg in self.messages:
            if msg.epoch == epoch:
                totals[msg.category] = totals.get(msg.category, 0) + msg.bytes
        return totals

    def epoch_budget_bytes(self, epoch: int) -> int:
        return sum(m.bytes for m in self.messages if m.epoch == epoch and m.charged_to_budget)

    def epoch_workload_bytes(self, epoch: int) -> int:
        return sum(m.bytes for m in self.messages if m.epoch == epoch and not m.charged_to_budget)

    def epoch_total_bytes(self, epoch: int) -> int:
        return sum(m.bytes for m in self.messages if m.epoch == epoch)

    @property
    def setup_sample_vectors_bytes(self) -> int:
        return self._setup_sample_vectors_bytes

    def amortized_setup_sample_bytes_per_epoch(self) -> float:
        if self.num_epochs <= 0:
            return 0.0
        return float(self._setup_sample_vectors_bytes) / float(self.num_epochs)

    def epoch_budget_bytes_with_amortized_setup(self, epoch: int) -> float:
        if any(
            m.epoch == epoch and m.category == "setup_sample_vectors_amortized"
            for m in self.messages
        ):
            return float(self.epoch_budget_bytes(epoch))
        return float(self.epoch_budget_bytes(epoch)) + self.amortized_setup_sample_bytes_per_epoch()
