"""Framework-independent streaming engine.

Deliberately free of any Django import. The engine owns event validation,
aggregation, deduplication and durability; Django owns HTTP, serialisation and
the relational alarm record. Keeping the boundary strict means the hot path
carries no framework overhead, and the whole engine is testable without a
settings module, a database or a running event loop.
"""

from engine.config import EngineConfig, get_config
from engine.service import StreamingEngine

__all__ = ["EngineConfig", "StreamingEngine", "get_config"]
