"""
Kafka configuration shared by the producer and the consumer workers.

Values are read from the project convention file ``.env.messaging.{type_run}``
(searched from the current working directory, as the rest of the project does)
and can be overridden with environment variables of the same name.
"""
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

from dotenv import dotenv_values

logger = logging.getLogger("mcps1")

_IGNORED_DIRS = {".venv", "venv", ".git", "node_modules", "__pycache__"}


@lru_cache(maxsize=None)
def _find_env_file(file_name: str, base_dir: str) -> Optional[Path]:
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = sorted(d for d in dirs if d not in _IGNORED_DIRS)
        if file_name in files:
            return Path(root) / file_name
    return None


def load_env_file(file_name: str) -> Dict[str, str]:
    """Returns the key/values of ``file_name`` (empty dict when it does not exist)."""
    env_path = _find_env_file(file_name, os.path.abspath(""))
    if env_path is None:
        logger.warning(f"Configuration file {file_name} not found from {os.path.abspath('')}")
        return {}
    return {key: value for key, value in dotenv_values(env_path).items() if value is not None}


def _split_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: Optional[str]
    client_id: str
    # consumer
    consumer_topics: list[str]
    group_id: str
    auto_offset_reset: str
    poll_timeout_ms: int
    retry_backoff_seconds: float
    retry_backoff_max_seconds: float
    # producer
    producer_topic: str
    producer_timeout_seconds: float
    producer_retry_cooldown_seconds: float

    @classmethod
    def load(cls, type_run: str = "dev") -> "KafkaSettings":
        config = load_env_file(f".env.messaging.{type_run}")

        def pick(key: str, default: str) -> str:
            return os.getenv(key) or config.get(key) or default

        # Precedence: env KAFKA_BOOTSTRAP_SERVERS > config file > legacy env "bootstrap_servers"
        bootstrap_servers = (os.getenv("KAFKA_BOOTSTRAP_SERVERS")
                             or config.get("KAFKA_BOOTSTRAP_SERVERS")
                             or os.getenv("bootstrap_servers"))

        return cls(
            bootstrap_servers=bootstrap_servers or None,
            client_id=pick("KAFKA_CLIENT_ID", "mcps"),
            consumer_topics=_split_list(pick("CONSUMER_TOPICS", "update")),
            group_id=pick("CONSUMER_GROUP_ID", "mcps"),
            auto_offset_reset=pick("CONSUMER_AUTO_OFFSET_RESET", "earliest"),
            poll_timeout_ms=int(pick("CONSUMER_POLL_TIMEOUT", "1000")),
            retry_backoff_seconds=float(pick("CONSUMER_RETRY_BACKOFF_SECONDS", "2")),
            retry_backoff_max_seconds=float(pick("CONSUMER_RETRY_BACKOFF_MAX_SECONDS", "30")),
            producer_topic=pick("DEFAULT_PRODUCER_TOPIC", "report"),
            producer_timeout_seconds=float(pick("PRODUCER_TIMEOUT_SECONDS", "5")),
            producer_retry_cooldown_seconds=float(pick("PRODUCER_RETRY_COOLDOWN_SECONDS", "30")),
        )
