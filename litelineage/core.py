import sqlite3
import functools
import inspect
from datetime import datetime
from typing import List, Optional

DB_FILE = "lineage.db"

class LineageTracker:
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create the simple schema if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lineage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    process_name TEXT,
                    input_asset TEXT,
                    output_asset TEXT
                )
            """)

    def track(self, inputs: List[str], outputs: List[str]):
        """
        The Decorator: @tracker.track(inputs=[...], outputs=[...])
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # 1. Run the actual function
                result = func(*args, **kwargs)
                
                # 2. Log metadata AFTER success
                self._log(func.__name__, inputs, outputs)
                return result
            return wrapper
        return decorator

    def _log(self, process_name: str, inputs: List[str], outputs: List[str]):
        """Writes relationships to SQLite."""
        with sqlite3.connect(self.db_path) as conn:
            timestamp = datetime.now().isoformat()
            # Explode: If 2 inputs -> 1 output, that is 2 rows in our graph edge table
            for inp in inputs:
                for out in outputs:
                    conn.execute(
                        "INSERT INTO lineage (timestamp, process_name, input_asset, output_asset) VALUES (?, ?, ?, ?)",
                        (timestamp, process_name, inp, out)
                    )

# Default instance for easy import
tracker = LineageTracker()