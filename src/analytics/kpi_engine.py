"""
===========================================================
 KPI ENGINE ZA E-COMMERCE ANALITIKU (MongoDB Aggregations)
===========================================================

Ovaj modul pokazuje kako se iz MongoDB-a izvlače ključni
e-commerce KPI-jevi korišćenjem *Mongo Aggregation Pipeline*.

Mongo Aggregation Pipeline funkcioniše kao "mini SQL engine",
ali radi nad BSON dokumentima i nizovima, zato su faze pipeline-a
izuzetno važne da se razumeju:

-------------------------------------------------------------
1) $unwind
-------------------------------------------------------------
Raspakuje niz u dokumentu tako da svaki element niza postane
“poseban red”.

Primer:
"items": [
    {"price": 10},
    {"price": 20}
]

posle unwind:
{ items: {"price": 10} }
{ items: {"price": 20} }

Bez ovoga je NEMOGUĆE raditi grupisanje po item-ima.

-------------------------------------------------------------
2) $addFields (ili $project sa izražajima)
-------------------------------------------------------------
Dodaje novo polje koje se računa na osnovu postojećih polja.

Primer:
{"$addFields": {"revenue": {"$add": ["$items.price", "$items.freight_value"]}}}

Mongo ovde računa "price + freight_value" za svaki item.

-------------------------------------------------------------
3) $group
-------------------------------------------------------------
Ovo je Mongo ekvivalent SQL-a:
GROUP BY kolona
    SUM(...) AS ...
    AVG(...)
    COUNT(...)

Primer:
{"$group": { "_id": "$customer.state", "total_revenue": {"$sum": "$revenue"} }}

-------------------------------------------------------------
4) $project
-------------------------------------------------------------
Biramo šta ulazi u finalni output (kao SQL SELECT).

-------------------------------------------------------------
5) $sort / $limit
-------------------------------------------------------------
Sortiranje i limit rezultata.

-------------------------------------------------------------
BITNO:
Sve KPI funkcije u ovom modulu zavise od strukture dokumenta
generisane u transformaciji:

{
    "_id": "order_id",
    "order": {...},
    "customer": {...},
    "items": [ {...}, {...} ],
    "payments": [ {...} ]
}

Ako se dokument promeni, ove putanje moraju da se usklade.
===========================================================
"""

from pymongo.collection import Collection
from src.load.load_to_mongo import get_orders_collection


def _col() -> Collection:
    """Interna pomoćna funkcija — dobavlja Mongo kolekciju."""
    return get_orders_collection()


# ===========================================================
# KPI: Prihod po kategoriji proizvoda
# ===========================================================
"""
Ovaj KPI računa:
- ukupni prihod (price + freight_value)
- po kategoriji proizvoda

Pipeline:
1) $unwind "$items"
   → svaki item postaje poseban dokument

2) $addFields { revenue: price + freight }
   → računamo prihod na nivou item-a

3) $group { _id: "$items.product_category", total_revenue: sum(revenue) }
   → sabiramo po kategoriji

4) $sort { total_revenue: -1 }
   → sortiramo kategorije po prihodu

Preuslov:
- items mora da bude niz
- svaki item mora imati price, freight_value, product_category
"""
def revenue_by_category(limit: int = 10):
    pipeline = [
        {"$unwind": "$items"},
        {"$addFields": {
            "revenue": {"$add": ["$items.price", "$items.freight_value"]}
        }},
        {"$group": {
            "_id": "$items.product_category",
            "total_revenue": {"$sum": "$revenue"},
            "items_count": {"$sum": 1}
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit}
    ]
    return list(_col().aggregate(pipeline))


# ===========================================================
# KPI: Prihod po državi kupca
# ===========================================================
"""
Pipeline je sličan kao prethodni, ali grupišemo po
"$customer.state" umesto po kategoriji.

Koraci:
1) $unwind "$items"
2) $addFields → revenue = price + freight
3) $group { _id: "$customer.state", total_revenue: sum(...) }
4) $sort

Neobavezno:
Može se dodati i broj porudžbina po državi.
"""
def revenue_by_state(limit: int = 10):
    pipeline = [
        {"$unwind": "$items"},
        {"$addFields": {
            "revenue": {"$add": ["$items.price", "$items.freight_value"]}
        }},
        {"$group": {
            "_id": "$customer.state",
            "total_revenue": {"$sum": "$revenue"}
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit}
    ]
    return list(_col().aggregate(pipeline))


# ===========================================================
# KPI: Prosečno kašnjenje isporuke
# ===========================================================
"""
Ovaj KPI meri:
- prosečan broj dana kašnjenja
  delay = delivered_date - estimated_date

Pipeline:
1) $match → filtriramo samo dokumente koji imaju oba datuma
2) $addFields { delay: (delivered - estimated) / (milisekunde u danima) }
3) $group { avg_delay: avg(delay) }

Mongo radi sa BSON datetime → možemo da koristimo $subtract direktno!
"""
def avg_delivery_delay():
    pipeline = [
        {"$match": {
            "order.delivered_customer_date": {"$ne": None},
            "order.estimated_delivery_date": {"$ne": None},
        }},
        {"$addFields": {
            "delay": {
                "$divide": [
                    {"$subtract": [
                        "$order.delivered_customer_date",
                        "$order.estimated_delivery_date"
                    ]},
                    1000 * 60 * 60 * 24  # ms → dani
                ]
            }
        }},
        {"$group": {
            "_id": None,
            "avg_delay": {"$avg": "$delay"}
        }}
    ]
    res = list(_col().aggregate(pipeline))
    return res[0] if res else None


# ===========================================================
# KPI: Repeat Customer Rate (ponavljani kupci)
# ===========================================================
"""
Cilj:
- izračunati procenat kupaca koji su kupili 2+ puta

Pipeline:
1) $group po "$customer.id"
   → brojimo porudžbine po kupcu

Rezultat:
{ "_id": "C123", "orders_count": 4 }

2) Ponovo $group — sada agregiramo:
   - total_customers
   - repeat_customers (orders_count >= 2 ? 1 : 0)

3) $project → računamo procenat

Ovo je Mongo verzija SQL-a:

WITH counts AS (
  SELECT customer_id, COUNT(*) AS orders_count
  FROM orders
  GROUP BY customer_id
)
SELECT
  SUM(orders_count >= 2) / COUNT(*)
FROM counts
"""
def repeat_customer_rate():
    pipeline = [
        {"$group": {
            "_id": "$customer.id",
            "orders_count": {"$sum": 1}
        }},
        {"$group": {
            "_id": None,
            "total_customers": {"$sum": 1},
            "repeat_customers": {
                "$sum": {
                    "$cond": [{"$gte": ["$orders_count", 2]}, 1, 0]
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "repeat_customer_rate_pct": {
                "$multiply": [
                    {"$divide": ["$repeat_customers", "$total_customers"]},
                    100
                ]
            }
        }}
    ]
    res = list(_col().aggregate(pipeline))
    return res[0] if res else None


# ===========================================================
# KPI: Payment Mix (kreditna kartica, boleto, voucher…)
# ===========================================================
"""
Ovaj KPI računa:
- koliko se svaka metoda plaćanja koristi
- koliko novca generiše
- % učešća u ukupnom broju uplata

Pipeline:
1) $unwind "$payments"
   → svaka uplata postaje zaseban dokument

2) $group po "$payments.payment_type"
   → count, total_value

3) $group → sabiramo ukupan broj uplata (total)

4) $unwind "$methods"
   → vraćamo nazad pojedinačne metode

5) $project → računamo procenat od total

6) $sort → najčešće metode prvo
"""
def payment_mix():
    pipeline = [
        {"$unwind": "$payments"},
        {"$group": {
            "_id": "$payments.payment_type",
            "count": {"$sum": 1},
            "total_value": {"$sum": "$payments.payment_value"}
        }},
        {"$group": {
            "_id": None,
            "total": {"$sum": "$count"},
            "methods": {
                "$push": {
                    "payment_type": "$_id",
                    "count": "$count",
                    "total_value": "$total_value"
                }
            }
        }},
        {"$unwind": "$methods"},
        {"$project": {
            "_id": 0,
            "payment_type": "$methods.payment_type",
            "count": "$methods.count",
            "total_value": "$methods.total_value",
            "percentage": {
                "$multiply": [
                    {"$divide": ["$methods.count", "$total"]},
                    100
                ]
            }
        }},
        {"$sort": {"percentage": -1}}
    ]
    return list(_col().aggregate(pipeline))


# ===========================================================
# FUNKCIJA KOJA VRAĆA SVE KPI-JE NA JEDNOM MESTU
# ===========================================================
"""
Vraća kompletan set KPI-jeva, spreman za dashboard, API ili izveštaje.
"""
def get_all_kpis() -> dict:
    return {
        "revenue_by_category": revenue_by_category(),
        "revenue_by_state": revenue_by_state(),
        "avg_delivery_delay": avg_delivery_delay(),
        "repeat_customer_rate": repeat_customer_rate(),
        "payment_mix": payment_mix(),
    }
