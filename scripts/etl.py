#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# -------------------- Formats validation --------------------
def validate_formats(df: pd.DataFrame, forbid_future_ts: bool = True) -> dict:
    report = {}
    req = ["timestamp","user_id","client_type","symbol","side","quantity","price"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        report["missing_columns"] = pd.DataFrame({"missing": miss})
        return report

    # Normalize
    df = df.copy()
    # Parse timestamp softly first to identify bad
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    bad_ts = ts.isna()
    if bad_ts.any():
        report["invalid_timestamp_format"] = df.loc[bad_ts, req]

    if forbid_future_ts:
        now = pd.Timestamp.utcnow().tz_localize(None)
        fut = ~bad_ts & (ts > now)
        if fut.any():
            report["timestamp_in_future"] = df.loc[fut, req]

    # Numeric checks
    qty = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["price"], errors="coerce")
    bad_qty = qty.isna() | (qty <= 0) | (qty % 1 != 0)
    bad_price = price.isna() | (price <= 0)
    if bad_qty.any():
        report["invalid_quantity"] = df.loc[bad_qty, req]
    if bad_price.any():
        report["invalid_price"] = df.loc[bad_price, req]

    # Categoricals
    side = df["side"].astype(str).str.strip().str.lower()
    client = df["client_type"].astype(str).str.strip().str.lower()
    allowed_side = {"buy","sell"}
    allowed_client = {"gold","silver","bronze"}
    bad_side = ~side.isin(allowed_side)
    bad_client = ~client.isin(allowed_client)
    if bad_side.any():
        report["invalid_side"] = df.loc[bad_side, req]
    if bad_client.any():
        report["invalid_client_type"] = df.loc[bad_client, req]

    return report

# -------------------- Helpers --------------------
def handle_null_timestamps(df: pd.DataFrame, mode: str = "error") -> pd.DataFrame:
    if "timestamp" not in df.columns:
        raise ValueError("Column 'timestamp' not found")
    # detect empties or NA before parsing
    empties = df["timestamp"].isna() | (df["timestamp"].astype(str).str.strip() == "")
    if empties.any():
        if mode == "error":
            idx = list(df.index[empties])[:5]
            raise ValueError(f"Found NULL/empty timestamps at rows: {idx} ...")
        elif mode == "drop":
            df = df.loc[~empties].copy()
        else:
            raise ValueError(f"Unknown mode: {mode}")
    return df

def to_week_start_monday(ts):
    # simplified: keep for compatibility but not used
    s = pd.to_datetime(ts, errors='coerce')
    return s.dt.to_period('W-MON').dt.start_time

# -------------------- ETL core --------------------
def extract(path: str) -> pd.DataFrame:
    df = pd.read_csv("data/trades.csv", parse_dates=['timestamp'])
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def transform(df: pd.DataFrame, include_pnl: bool, null_ts_mode: str) -> pd.DataFrame:
    df = df.copy()
    # validation preview (not failing)
    _ = validate_formats(df)

    # handle NULL timestamps per mode
    df = handle_null_timestamps(df, mode=null_ts_mode)
    # parse + normalize
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.loc[:, "client_type"] = df["client_type"].astype("string").str.lower().str.strip()
    df.loc[:, "symbol"] = df["symbol"].astype("string").str.upper().str.strip()
    df.loc[:, "side"] = df["side"].astype("string").str.lower().str.strip()
    # numeric
    df.loc[:, "quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df.loc[:, "price"] = pd.to_numeric(df["price"], errors="coerce")

    # drop still-bad rows (rare, but safe)
    df = df.dropna(subset=["timestamp","quantity","price"]).copy()

    # only allowed client types
    df = df[df["client_type"].isin({"gold","silver","bronze"})].copy()

    # week start
    df.loc[:, "week_start_date"] = to_week_start_monday(df["timestamp"])

    # PnL
    if include_pnl:
        df.loc[:, "cashflow"] = df.apply(
            lambda r: r["price"] * r["quantity"] * (1 if r["side"] == "sell" else -1),
            axis=1
        )

    # aggregate
    group_cols = ["week_start_date","client_type","user_id","symbol"]
    agg_dict = {
        "quantity": "sum",
        "symbol": "size",
        "price": "mean",
    }
    agg = df.groupby(group_cols, dropna=False).agg(agg_dict).rename(
        columns={"quantity":"total_volume","symbol":"trade_count","price":"price"}
    ).reset_index()

    if include_pnl and "cashflow" in df.columns:
        pnl = df.groupby(group_cols)["cashflow"].sum().reset_index().rename(columns={"cashflow":"total_pnl"})
        agg = agg.merge(pnl, on=group_cols, how="left")

    return agg

def load(df: pd.DataFrame, db_path: str, table: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)

# -------------------- Reporting --------------------
def export_top_bronze_exact(agg_df: pd.DataFrame, out_csv: str = "output/top_clients.csv"):
    bronze = agg_df[agg_df["client_type"] == "bronze"].copy()

    top_clients = (
        bronze.groupby("user_id")
              .agg(total_volume=("total_volume", "sum"),
                   total_pnl=("total_pnl", "sum"))
              .sort_values("total_volume", ascending=False)
              .head(3)
    )

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    top_clients.to_csv(out_csv, index=True)

def plot_weekly_volume(agg_df: pd.DataFrame, png_path: str = "output/weekly_volume.png"):
    if agg_df.empty:
        return

    plt.figure(figsize=(10, 6))
    
    for ctype, grp in agg_df.groupby("client_type"):
        weekly = grp.groupby("week_start_date")["total_volume"].sum()
        if weekly.empty:
            continue
        plt.plot(
            weekly.index,
            weekly.values,
            marker="o",
            label=ctype.capitalize()
        )

    plt.title(
        "Total Volume by Week & Client Type",
        fontname="Montserrat", fontsize=16, fontweight="bold"
    )
    plt.xlabel("Week Start Date", fontname="Inter", fontsize=12)
    plt.ylabel("Total Volume", fontname="Inter", fontsize=12) 
    plt.legend(prop={"family": "Inter", "size": 10})
    plt.xticks(fontname="Inter", fontsize=9, rotation=30)
    plt.yticks(fontname="Inter", fontsize=9)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    Path(png_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(png_path, dpi=300)
    plt.close()


def plot_symbols_total_volume(agg_df: pd.DataFrame, png_path: str = "output/symbols_total_volume.png"):
    from pathlib import Path
    import matplotlib.pyplot as plt

    if agg_df.empty:
        return

    top_symbols = (
        agg_df.groupby("symbol")["total_volume"]
              .sum()
              .sort_values(ascending=False)
    )

    if top_symbols.empty:
        return

    plt.figure(figsize=(8, 5))
    bars = plt.bar(top_symbols.index, top_symbols.values, label="Total Volume")

    plt.title("Symbols by Total Volume",
              fontname="Montserrat", fontsize=16, fontweight="bold")
    plt.xlabel("Symbol", fontname="Inter", fontsize=12)
    plt.ylabel("Total Volume", fontname="Inter", fontsize=12)
    plt.xticks(fontname="Inter", rotation=30, ha="right")
    plt.legend(prop={"family": "Inter", "size": 10})

    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, h, f"{h:,.0f}",
                 ha="center", va="bottom", fontname="Inter", fontsize=9)

    plt.tight_layout()
    Path(png_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(png_path, dpi=300)
    plt.close()

# -------------------- Orchestrator --------------------
def run_etl(data_path, db_path, table, out_top, chart_weekly_path, chart_symbols_path, include_pnl, null_ts_mode):
    raw = extract(data_path)
    agg = transform(raw, include_pnl=include_pnl, null_ts_mode=null_ts_mode)
    load(agg, db_path, table)

    export_top_bronze_exact(agg, out_csv=f"{out_top}.csv")

    plot_weekly_volume(agg, chart_weekly_path)
    plot_symbols_total_volume(agg, chart_symbols_path)

    print(f"Loaded {len(agg)} rows into {db_path}:{table}")
    print(f"Top-3 bronze reports at {out_top}.csv/.xlsx")
    print("Charts saved to:")
    print(f"  - {chart_weekly_path}")
    print(f"  - {chart_symbols_path}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data/trades.csv")
    p.add_argument("--db", default="output/agg_result.db")
    p.add_argument("--table", default="agg_trades_weekly")
    p.add_argument("--top", default="output/top_clients")
    p.add_argument("--chart-weekly",  default="output/weekly_volume.png")
    p.add_argument("--chart-symbols", default="output/symbols_total_volume.png")
    p.add_argument("--no-pnl", action="store_true", help="Disable naive cashflow PnL")
    p.add_argument("--null-timestamp", choices=["error","drop","fill"], default="error",
                   help="How to handle NULL/empty timestamps")
    args = p.parse_args()

    run_etl(
        args.data,
        args.db,
        args.table,
        args.top,
        args.chart_weekly,
        args.chart_symbols,
        include_pnl=not args.no_pnl,
        null_ts_mode=args.null_timestamp
    )

if __name__ == "__main__":
    main()
