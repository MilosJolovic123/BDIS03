from pathlib import Path

#Putanja do foldera sa sirovim CSV fajlovima

#Path funkcija uzima putanju do ovog fajla dok ga resolve pretvara u apsolutnu putanju, onda idemo jedan nivo iznad sa parents
BASE_DIR = Path(__file__).resolve().parents[1]
# ovde uzimamo BASE_DIR plus folder u kojima su nam raw podaci
DATA_RAW_DIR = BASE_DIR / "data_raw"

#Podesavanja za Mongo
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB_NAME = "olist_ecommerce"
MONGO_ORDERS_COLLECTION = "orders"