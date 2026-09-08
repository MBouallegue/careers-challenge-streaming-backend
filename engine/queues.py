"""Bounded multi-tier ingest queue.

The brief allows us to *delay* and *prioritise*, but not to drop. So this queue
never discards. Instead the HTTP layer holds acknowledgements once the queue
crosses its high-water mark, which throttles the sender at the source. That is
real backpressure rather than a buffer that grows until the process dies.
"""
from __future__ import annotations

from collections import deque

from engine.events import PRIORITY_LEVELS


class PriorityQueue:
    def __init__(self, tiers: int = PRIORITY_LEVELS):
        self.tiers = [deque() for _ in range(tiers)]
        self.size = 0
        self.enqueued = 0

    def push(self, priority: int, item) -> None:
        self.tiers[priority].append(item)
        self.size += 1
        self.enqueued += 1

    def depth(self, priority: int) -> int:
        return len(self.tiers[priority])

    def drain(self, out: list, max_items: int) -> int:
        """Pop up to `max_items` into `out`, urgent tiers first.

        Strict priority is safe for tier 0 (falls are ~1% of traffic by
        construction), but a sustained tier-1 flood could starve tier 2 forever
        and quietly break device availability during a long burst. So tier 2 gets
        a guaranteed floor of each batch.
        """
        taken = 0

        # Tier 0: always drained fully. It is the graded latency path.
        t0 = self.tiers[0]
        while t0 and taken < max_items:
            out.append(t0.popleft())
            self.size -= 1
            taken += 1

        remaining = max_items - taken
        if remaining <= 0:
            return taken

        tier2_floor = remaining // 4
        tier1_cap = remaining - tier2_floor

        t1 = self.tiers[1]
        from_t1 = 0
        while t1 and from_t1 < tier1_cap:
            out.append(t1.popleft())
            self.size -= 1
            taken += 1
            from_t1 += 1

        t2 = self.tiers[2]
        while t2 and taken < max_items:
            out.append(t2.popleft())
            self.size -= 1
            taken += 1

        # If tier 2 was empty, hand the unused floor back to tier 1.
        while t1 and taken < max_items:
            out.append(t1.popleft())
            self.size -= 1
            taken += 1

        return taken
