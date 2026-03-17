"""
DataDumper: Thread-safe utility for dumping request input/output data to JSONL files.
"""

import hashlib
import json
import logging
import os
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


class DataDumper:
    """Thread-safe JSONL writer for dumping request data."""

    def __init__(self, dump_dir: str, server_args_dict: dict):
        self.dump_dir = dump_dir
        os.makedirs(dump_dir, exist_ok=True)

        # Generate filename: {deployment_time}_{params_hash}.jsonl
        deployment_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        params_hash = self._hash_params(server_args_dict)
        self.dump_file = os.path.join(
            dump_dir, f"{deployment_time}_{params_hash}.jsonl"
        )

        # Thread-safe lock for concurrent writes
        self._lock = threading.Lock()

        logger.info(f"DataDumper initialized. Dumping to: {self.dump_file}")

    @staticmethod
    def _hash_params(params: dict) -> str:
        """Generate a short hash from server parameters."""
        params_str = json.dumps(params, sort_keys=True, default=str)
        return hashlib.md5(params_str.encode()).hexdigest()[:8]

    def dump_record(self, record: dict):
        """Thread-safe write of a single record to the JSONL file."""
        try:
            line = json.dumps(record, ensure_ascii=False) + "\n"
            with self._lock:
                with open(self.dump_file, "a", encoding="utf-8") as f:
                    f.write(line)
        except Exception as e:
            logger.error(f"DataDumper failed to write record: {e}")
