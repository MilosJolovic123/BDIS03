from pathlib import Path
import pandas as pd
from config.settings import DATA_RAW_DIR


def load_orders() -> pd.DataFrame:
    path = DATA_RAW_DIR / "olist_orders_dataset.csv"
    return pd.read_csv(path)

def load_order_items() -> pd.DataFrame:
    path = DATA_RAW_DIR / "olist_order_items_dataset.csv"
    return pd.read_csv(path)

def load_customers() -> pd.DataFrame:
    path = DATA_RAW_DIR / "olist_customers_dataset.csv"
    return pd.read_csv(path)

def load_products() -> pd.DataFrame:
    path = DATA_RAW_DIR / "olist_products_dataset.csv"
    return pd.read_csv(path)

def load_payments() -> pd.DataFrame:
    path = DATA_RAW_DIR / "olist_order_payments_dataset.csv"
    return pd.read_csv(path)

def load_all_raw():
    orders = load_orders()
    order_items = load_order_items()
    customers = load_customers()
    products = load_products()
    payments = load_payments()

    return {
        "orders": orders,
        "order_items":order_items,
        "customers":customers,
        "products": products,
        "payments": payments
    }