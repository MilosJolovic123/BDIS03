## 🛒 Uvod u projekat: Python + MongoDB E-Commerce Analitika

Ovaj projekat demonstrira studijski primer integracije Python programskog jezika i MongoDB document-oritented baze podataka. Ideja je jednostavna: 
uzeti realan e-commerce dataset (Olist), transformisati ga u 
MongoDB dokumente, i zatim nad njima izračunati ključne poslovne
KPI-jeve koji se koriste u praksi.

### 🔍 Šta radimo u projektu?

1. **Početni podaci (CSV)**  
   Dobijamo više tabela (orders, customers, order_items, payments, products).  
   To je tipičan relacijski model sa odvojenim tabelama.

2. **Transformacija u dokument model**  
   Umesto spajanja u SQL-u, koristimo Python + Pandas da:  
   - spojimo sve tabele,  
   - grupišemo sve stavke (`items`) i uplate (`payments`) u liste,  
   - ugnjezdimo kupca (`customer`) i porudžbinu (`order`) u jedan dokument.  
   
   Rezultat: **jedan dokument = jedna porudžbina**, idealno za MongoDB.

3. **Upis u MongoDB**  
   Dobijeni dokumenti se upisuju u Mongo kolekciju.  
   MongoDB dobro radi sa ugnježdenim strukturama, pa su kasniji upiti brži
   i jednostavniji nego u relacijskim bazama.

4. **Analitika pomoću Mongo Aggregation Pipeline-a**  
   Koristimo `$unwind`, `$group`, `$addFields` i `$sort` da izračunamo:
   - prihod po kategoriji proizvoda  
   - prihod po državi kupca  
   - prosečno kašnjenje isporuke  
   - procenat ponovljenih kupovina (repeat customers)  
   - distribuciju metoda plaćanja  

### 🎯 Zašto MongoDB?

SQL čuva podatke u tabelama i zahteva JOIN operacije.  
Mongo čuva sve relevantne informacije u **jednom dokumentu**, što znači:

- nema multiple JOIN-ova  
- brže se izvode agregacije  
- jednostavnije je modelovati kompleksne strukture (items, payments…)  
- pogodnije za e-commerce i analitičke use-case-ove

### 📚 Novo od gradiva:

- kako izgleda end-to-end ETL pipeline  
- kako se relacioni podaci pretvaraju u NoSQL dokument  
- kako se kreiraju ugnježdene strukture sa Python-om  
- kako Mongo Aggregation Pipeline funkcioniše u praksi  
- kako nastaju poslovni KPI-jevi i kako ih implementirati iz podataka  

Ovo je mini-projekat koji kombinuje praktično znanje iz Python-a,
NoSQL baza, data transformacija i e-commerce analitike, i predstavlja
baznu vežbu iz modernog data engineering procesa.
