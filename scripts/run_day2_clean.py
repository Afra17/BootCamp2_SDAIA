import logging
import sys
from pathlib import Path
from Data_profiler.quality import assert_in_range
from Data_profiler.config import make_paths
from Data_profiler.io import read_orders_csv,read_users_csv ,write_parquet
from Data_profiler.transforms import (
    enforce_schame,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from Data_profiler.quality import (
    require_columns,
    assert_non_empty,
)

log = logging.getLogger(__name__)
def main() -> None:
    root = Path(__file__).resolve().parents[1]
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = make_paths(root)

    log.info("Loading raw inputs")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    log.info("Rows: orders_raw=%s, users=%s", len(orders_raw), len(users))

    require_columns(orders_raw, ["order_id","user_id","amount","quantity","created_at","status"])
    require_columns(users, ["user_id","country","signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    orders = enforce_schame(orders_raw)

    # Missingness artifact (do this early — before you “fix” missing values)
    rep = missingness_report(orders)
    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "missingness_orders.csv"
    rep.to_csv(rep_path, index=True)
    log.info("Wrote missingness report: %s", rep_path)

     # Text normalization + controlled mapping
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        orders.assign(status_clean=status_clean)
              .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    # Task 7: add at least one `assert_in_range(...)` check here (fail fast)

    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    log.info("Wrote processed outputs: %s", p.processed)
    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")




if __name__ == "__main__":
    main()