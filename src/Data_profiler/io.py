import pandas as pd
from pathlib import Path
NA=["","","NA","N/A","na","n/a","None","not_a_number"]
def read_order_csv(path) -> pd.DataFrame:
  return pd.read_csv(
    path,
    dtype={"order_id":"string","user_id":"string"},
    na_values =NA,
    keep_default_na=True
  )               

def read_user_csv(path) -> pd.DataFrame:
  pass

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


df=read_order_csv(r"C:\Users\user\OneDrive - University of Prince Mugrin\سطح المكتب\Bootcamp2\data\order.csv")

path1=Path(r"C:\Users\user\OneDrive - University of Prince Mugrin\سطح المكتب\Bootcamp2\data\processed\clean_order.parquet")
a=write_parquet(df,path1)