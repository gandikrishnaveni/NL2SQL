"""
Shared extensions and cached resources.
Using a simple dict cache keyed by db_path so we
don't re-instantiate NLPEngine on every request.
"""
from threading import Lock

_engine_cache: dict = {}
_engine_lock = Lock()


def get_engine(db_path: str):
    """Return a cached NLPEngine for the given db_path."""
    if db_path not in _engine_cache:
        with _engine_lock:
            if db_path not in _engine_cache:
                from nlp_engine import NLPEngine  # project import
                _engine_cache[db_path] = NLPEngine(db_path)
    return _engine_cache[db_path]


# thin alias kept for clarity in other modules
db_session = None  # no ORM needed; raw sqlite used everywhere