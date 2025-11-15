import pandas as pd

def parse_dt(series: pd.Series) -> pd.Series:
    """
    Pretvaramo string u datetime.
    Ako je vrednost loša ili prazna, dobijamo NaT (Not a Time).
    Ovo je važno jer MongoDB lepo radi sa datetime tipovima.
    """
    return pd.to_datetime(series, errors="coerce")


def clean_and_join(raw: dict) -> pd.DataFrame:
    """
    Ulaz: dict sa DataFrame-ovima: orders, order_items, customers, products, payments.
    Izlaz: DataFrame gde je SVAKI RED jedna porudžbina,
           sa kolonom 'items' (lista dict-ova) i 'payments' (lista dict-ova),
           spreman da se pretvori u Mongo dokument.
    """

    orders = raw["orders"].copy()
    order_items = raw["order_items"].copy()
    customers = raw["customers"].copy()
    products = raw["products"].copy()
    payments = raw["payments"].copy()

    # 1. Datum kolone u orders pretvaramo u datetime
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        orders[col] = parse_dt(orders[col])

    # 2. Merge orders + customers
    #    Ovo je kao SQL JOIN, pravimo "osnovnu" tabelu gde svaka porudžbina ima kupca.
    df = orders.merge(customers, on="customer_id", how="left")

    # 3. Merge order_items + products
    #    Ovde "obogaćujemo" stavke porudžbine informacijom o kategoriji proizvoda.
    items_merged = order_items.merge(
        products[["product_id", "product_category_name"]],
        on="product_id",
        how="left"
    )
    items_merged["product_category_name"] = \
        items_merged["product_category_name"].fillna("unknown")

    # 4. Grupisanje stavki po porudžbini → lista dict-ova (Mongo friendly struktura)
    def build_items_list(group: pd.DataFrame) -> list:
        """
        Ulaz: grupa redova order_items+products za jedan order_id.
        Izlaz: lista dict-ova: [{product_id, category, price, freight}, ...]
        """
        return [
            {
                "order_item_id": int(row["order_item_id"]),
                "product_id": row["product_id"],
                "product_category": row["product_category_name"],
                "price": float(row["price"]),
                "freight_value": float(row["freight_value"]),
            }
            for _, row in group.iterrows()
        ]

    items_grouped = (
        items_merged
        .groupby("order_id")
        .apply(build_items_list)
        .rename("items")
        .reset_index()
    )

    # 5. Grupisanje uplata po porudžbini → lista dict-ova
    def build_payments_list(group: pd.DataFrame) -> list:
        """
        Ulaz: grupa redova payments za jedan order_id.
        Izlaz: lista dict-ova: [{payment_type, installments, value}, ...]
        """
        return [
            {
                "payment_sequential": int(r["payment_sequential"]),
                "payment_type": r["payment_type"],
                "payment_installments": int(r["payment_installments"]),
                "payment_value": float(r["payment_value"]),
            }
            for _, r in group.iterrows()
        ]

    payments_grouped = (
        payments
        .groupby("order_id")
        .apply(build_payments_list)
        .rename("payments")
        .reset_index()
    )

    # 6. Merge items i payments nazad na "osnovnu" tabelu porudžbina
    df = df.merge(items_grouped, on="order_id", how="left")
    df = df.merge(payments_grouped, on="order_id", how="left")

    # 7. Obezbeđujemo da su items/payments uvek liste, ne NaN
    df["items"] = df["items"].apply(lambda x: x if isinstance(x, list) else [])
    df["payments"] = df["payments"].apply(lambda x: x if isinstance(x, list) else [])

    return df

# Helper funkcija za sigurno kastovnje nan vrednosti u None 
# inace ce Mongo da baci gresku
def safe_dt(value):
    """Ako je pandas.NaT → vrati None. Inače vrati originalnu vrednost."""
    if pd.isna(value):
        return None
    return value

def df_to_mongo_docs(df: pd.DataFrame) -> list[dict]:
    """
    Uzima transformisani DataFrame (jedan red = jedna porudžbina)
    i pretvara ga u listu Mongo dokumenata (dict-ova)
    sa ugnježdenim strukturama.
    """
    documents = []

    for _, row in df.iterrows():
        doc = {
            "_id": row["order_id"],  # order_id koristimo kao primarni ključ u kolekciji
            "order": {
                "status": row["order_status"],
                "purchase_timestamp": safe_dt(row["order_purchase_timestamp"]),
                "approved_at": safe_dt(row["order_approved_at"]),
                "delivered_customer_date": safe_dt(row["order_delivered_customer_date"]),
                "estimated_delivery_date": safe_dt(row["order_estimated_delivery_date"]),
            },
            "customer": {
                "id": row["customer_id"],
                "unique_id": row["customer_unique_id"],
                "zip_code_prefix": row["customer_zip_code_prefix"],
                "city": str(row["customer_city"]).title() if pd.notna(row["customer_city"]) else None,
                "state": str(row["customer_state"]).upper() if pd.notna(row["customer_state"]) else None,
            },
            # Ovo su direktno liste dict-ova koje smo ranije napravili
            "items": row["items"],
            "payments": row["payments"],
        }
        documents.append(doc)

    return documents
