from src.mongo_client import get_db
from config.settings import MONGO_ORDERS_COLLECTION

def get_orders_collection():
    return get_db()[MONGO_ORDERS_COLLECTION]

def reset_orders_collection():
    col = get_orders_collection()
    col.drop()
    col.create_index("order.status")
    col.create_index("customer.state")
    col.create_index("customer.city")
    col.create_index("items.product_category")
    col.create_index("order.purchase_timestamp")

def bulk_insert_orders(documents: list):
    if documents:
        get_orders_collection().insert_many(documents, ordered=False)