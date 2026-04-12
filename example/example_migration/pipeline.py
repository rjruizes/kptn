import kptn
import duckdb
from pathlib import Path

DATA_DB = "output/data.duckdb"


def _ensure_output() -> None:
    Path("output").mkdir(exist_ok=True)


@kptn.task(outputs=[f"duckdb://{DATA_DB}::init_status"])
def init_database() -> None:
    _ensure_output()
    conn = duckdb.connect(DATA_DB)
    conn.execute("CREATE TABLE IF NOT EXISTS init_status (ts TEXT)")
    conn.execute("DELETE FROM init_status")
    conn.execute("INSERT INTO init_status VALUES (current_timestamp::TEXT)")
    conn.commit()
    conn.close()


@kptn.task(outputs=[f"duckdb://{DATA_DB}::ref_data"])
def update_ref() -> None:
    conn = duckdb.connect(DATA_DB)
    conn.execute("CREATE TABLE IF NOT EXISTS ref_data (key TEXT, val TEXT)")
    conn.execute("DELETE FROM ref_data")
    conn.execute("INSERT INTO ref_data VALUES ('version', '1.0')")
    conn.commit()
    conn.close()


@kptn.task(outputs=[f"duckdb://{DATA_DB}::base_data"])
def load_base_data() -> None:
    conn = duckdb.connect(DATA_DB)
    conn.execute("CREATE TABLE IF NOT EXISTS base_data (subject_id TEXT, enrolled BOOLEAN)")
    conn.execute("DELETE FROM base_data")
    conn.execute("INSERT INTO base_data VALUES ('subj_001', true)")
    conn.commit()
    conn.close()


@kptn.task(outputs=[f"duckdb://{DATA_DB}::ds1"])
def load_ds1(steps_override: int = 8500) -> None:
    conn = duckdb.connect(DATA_DB)
    conn.execute("CREATE TABLE IF NOT EXISTS ds1 (subject_id TEXT, steps INTEGER)")
    conn.execute("DELETE FROM ds1")
    conn.execute("INSERT INTO ds1 VALUES ('subj_001', ?)", [steps_override])
    conn.commit()
    conn.close()


@kptn.task(outputs=[f"duckdb://{DATA_DB}::ds2"])
def load_ds2() -> None:
    conn = duckdb.connect(DATA_DB)
    conn.execute("CREATE TABLE IF NOT EXISTS ds2 (subject_id TEXT, score DOUBLE)")
    conn.execute("DELETE FROM ds2")
    conn.execute("INSERT INTO ds2 VALUES ('subj_001', 72.4)")
    conn.commit()
    conn.close()


graph = init_database >> update_ref >> load_base_data >> kptn.Stage("datasets", load_ds1, load_ds2)
pipeline = kptn.Pipeline("example_flow", graph)


if __name__ == "__main__":
    kptn.run(pipeline)
