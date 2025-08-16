import os
import pytest

@pytest.mark.skipif(not os.getenv("PGHOST"), reason="Database not configured")
def test_database_connection():
    from db import get_engine, ensure_schema, create_tables
    engine = get_engine()
    with engine.connect() as con:
        con.execute("SELECT 1")
    ensure_schema(engine)
    create_tables(engine)
