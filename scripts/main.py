from pathlib import Path
import sys
import json
from datetime import datetime, timezone
import logging
from Data_profiler.config import make_paths
from Data_profiler.io import read_orders_csv, read_users_csv, write_parquet
from Data_profiler.transforms import enforce_schame


# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

log = logging.getLogger(__name__)

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    p = make_paths(ROOT)
    orders = enforce_schame(read_orders_csv(p.raw / "order1.csv"))
    users = read_users_csv(p.raw / "user1.csv")

    log.info("Loaded rows: orders=%s users=%s", len(orders), len(users))
    log.info("Orders dtypes:\n%s", orders.dtypes)

    out_orders = p.processed / "order1.parquet"
    out_users = p.processed / "user1.parquet"
    write_parquet(orders, out_orders)
    write_parquet(users, out_users)

    meta = {  # Optional but useful: minimal run metadata for reproducibility
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows": {"orders": int(len(orders)), "users": int(len(users))},
        "outputs": {"orders": str(out_orders), "users": str(out_users)},
    }
    meta_path = p.processed / "_run_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    log.info("Wrote: %s", p.processed)
    log.info("Run meta: %s", meta_path)

if __name__ == "__main__":
    main()