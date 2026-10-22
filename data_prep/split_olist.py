import pandas as pd
from pathlib import Path

RAW_DIR = Path("olist_raw")  # your 9 Kaggle CSV files
OUT_DIR = Path("source_out")  # output, will be uploaded to 'source' container
WINDOW_START = pd.Timestamp("2018-06-01")
WINDOW_END = pd.Timestamp("2018-06-30")
HISTORY_DAY = (WINDOW_START - pd.Timedelta(days=1)).strftime("%Y-%m-%d")  # 2018-05-31


def write(df, table, day):
    day = str(day)[:10]
    folder = OUT_DIR / "olist" / table / f"ingestion_date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    df.to_csv(folder / f"{table}_{day.replace('-', '')}.csv", index=False)


def split_by_date(df, table):
    """df needs a helper column '_d' (date). Before window -> 1 history file. In window -> 1 file per day."""
    write(df[df["_d"] < WINDOW_START].drop(columns="_d"), table, HISTORY_DAY)
    win = df[(df["_d"] >= WINDOW_START) & (df["_d"] <= WINDOW_END)]
    for day, part in win.groupby("_d"):
        write(part.drop(columns="_d"), table, day.date())


# ---------- ORDERS (with updates) ----------
ts_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
event_cols = ts_cols[:4]  # real events (estimated date is not an event)

orders = pd.read_csv(RAW_DIR / "olist_orders_dataset.csv", parse_dates=ts_cols)
orders["last_updated_ts"] = orders[event_cols].max(axis=1)
orders["_final_d"] = orders["last_updated_ts"].dt.normalize()
orders["_buy_d"] = orders["order_purchase_timestamp"].dt.normalize()

# Event 2: final state, arrives on the day of the last event
final = orders.assign(_d=orders["_final_d"])

# Event 1: 'created' state on purchase day (only if the order changes on a later day)
created = orders[orders["_final_d"] > orders["_buy_d"]].copy()
created["order_status"] = "created"
for c in event_cols[1:]:
    created[c] = pd.NaT
created["last_updated_ts"] = created["order_purchase_timestamp"]
created["_d"] = created["_buy_d"]

events = pd.concat([created, final], ignore_index=True).drop(columns=["_final_d", "_buy_d"])

# History file = newest known state of each order before the window
hist = (
    events[events["_d"] < WINDOW_START]
    .sort_values("last_updated_ts")
    .drop_duplicates("order_id", keep="last")
)
write(hist.drop(columns="_d"), "orders", HISTORY_DAY)

win = events[(events["_d"] >= WINDOW_START) & (events["_d"] <= WINDOW_END)]
for day, part in win.groupby("_d"):
    write(part.drop(columns="_d"), "orders", day.date())


# ---------- ORDER ITEMS AND PAYMENTS (follow the order's purchase date) ----------
buy_date = orders.set_index("order_id")["_buy_d"]
for fname, table in [
    ("olist_order_items_dataset.csv", "order_items"),
    ("olist_order_payments_dataset.csv", "order_payments"),
]:
    df = pd.read_csv(RAW_DIR / fname)
    df["_d"] = df["order_id"].map(buy_date)
    split_by_date(df, table)


# ---------- REVIEWS (use review_creation_date) ----------
rev = pd.read_csv(
    RAW_DIR / "olist_order_reviews_dataset.csv",
    parse_dates=["review_creation_date", "review_answer_timestamp"],
)
rev["_d"] = rev["review_creation_date"].dt.normalize()
split_by_date(rev, "order_reviews")


# ---------- DIMENSIONS: one full snapshot ----------
dims = {
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}
for fname, table in dims.items():
    write(pd.read_csv(RAW_DIR / fname, dtype=str), table, HISTORY_DAY)  # dtype=str keeps zip codes as-is


# ---------- SIMULATED CUSTOMER ADDRESS CHANGES (for SCD2 later) ----------
customers = pd.read_csv(RAW_DIR / "olist_customers_dataset.csv", dtype=str)
places = customers[["customer_city", "customer_state"]].drop_duplicates()
for day in pd.date_range("2018-06-05", "2018-06-30", freq="5D"):
    chg = customers.sample(30, random_state=day.day).copy()
    new = places.sample(30, random_state=day.day + 1).to_numpy()
    chg["customer_city"], chg["customer_state"] = new[:, 0], new[:, 1]
    write(chg, "customers", day.date())


print("Done. CSV files created:", sum(1 for _ in OUT_DIR.rglob("*.csv")))
