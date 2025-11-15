# main.py
"""
Glavni ulaz u aplikaciju.

Radi dve stvari:
1) ETL pipeline: CSV -> Pandas -> MongoDB dokumenti
2) KPI dashboard: povlači KPI-jeve iz MongoDB-a i ispisuje ih

Pokrećeš ovako iz root foldera projekta:
    python main.py
"""

from src.extract.extract_data import load_all_raw
from src.transform.transform_data import clean_and_join, df_to_mongo_docs
from src.load.load_to_mongo import reset_orders_collection, bulk_insert_orders
from src.analytics.kpi_engine import get_all_kpis


def run_etl():
    """Pokreće kompletan ETL: čitanje CSV, transformacija, upis u MongoDB."""
    print("[ETL] Učitavam raw CSV fajlove...")
    raw = load_all_raw()

    print("[ETL] Čistim i spajam podatke u jedan DataFrame...")
    df_joined = clean_and_join(raw)
    print(f"[ETL] Broj porudžbina (redova): {len(df_joined)}")

    print("[ETL] Konvertujem DataFrame u Mongo dokumente...")
    docs = df_to_mongo_docs(df_joined)
    print(f"[ETL] Broj dokumenata za insert: {len(docs)}")

    print("[ETL] Resetujem Mongo kolekciju i kreiram indekse...")
    reset_orders_collection()

    print("[ETL] Ubacujem dokumente u MongoDB...")
    bulk_insert_orders(docs)

    print("[ETL] Gotovo. Podaci su u MongoDB-u.")


def run_kpi_dashboard():
    """Povlači i ispisuje sve KPI-jeve koje definiše kpi_engine."""
    print("\n=== E-COMMERCE KPI DASHBOARD (MongoDB) ===\n")
    kpis = get_all_kpis()

    for name, value in kpis.items():
        print(f"{name}:")
        print(value)
        print("-" * 50)


if __name__ == "__main__":
    run_etl()
    run_kpi_dashboard()
