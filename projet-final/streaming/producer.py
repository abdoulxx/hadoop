"""
Producteur Kafka "replay" : rejoue le dataset Online Retail trie par InvoiceDate,
en simulant un flux temps reel (avec un delai compresse entre les lignes).

Usage:
    python producer.py [--speedup 3600] [--max-sleep 2.0] [--limit 0] [--topic online-retail]

--speedup   : facteur de compression du temps simule (3600 = 1h simulee -> 1s reelle). Defaut 3600.
--max-sleep : delai maximum (secondes) entre deux lignes, meme si l'ecart simule est plus grand. Defaut 2.0.
--limit     : nombre max de lignes a envoyer (0 = tout le dataset). Defaut 0.
--topic     : topic Kafka cible. Defaut online-retail.
"""

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "online_retail.tsv"


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--speedup", type=float, default=3600.0)
    parser.add_argument("--max-sleep", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--topic", type=str, default="online-retail")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    args = parser.parse_args()

    print(f"Chargement de {DATA_PATH} ...")
    df = pd.read_csv(DATA_PATH, sep="\t")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df.sort_values("InvoiceDate").reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)
    print(f"{len(df)} lignes a rejouer, triees par InvoiceDate.")

    producer = build_producer(args.bootstrap_servers)

    prev_ts = None
    sent = 0
    t0 = time.time()
    try:
        for row in df.itertuples(index=False):
            ts = row.InvoiceDate
            if prev_ts is not None:
                delta_simule = (ts - prev_ts).total_seconds()
                delai = min(delta_simule / args.speedup, args.max_sleep)
                if delai > 0:
                    time.sleep(delai)
            prev_ts = ts

            message = {
                "invoice_no": str(row.InvoiceNo),
                "stock_code": str(row.StockCode),
                "description": None if pd.isna(row.Description) else str(row.Description),
                "quantity": int(row.Quantity),
                "invoice_date": ts.isoformat(),
                "unit_price": float(row.UnitPrice),
                "customer_id": None if pd.isna(row.CustomerID) else int(row.CustomerID),
                "country": str(row.Country),
            }
            producer.send(args.topic, key=message["country"], value=message)
            sent += 1
            if sent % 1000 == 0:
                elapsed = time.time() - t0
                print(f"  {sent}/{len(df)} lignes envoyees ({elapsed:.1f}s ecoulees, derniere date simulee {ts})")
    except KeyboardInterrupt:
        print("Interrompu par l'utilisateur.")
    finally:
        producer.flush()
        producer.close()
        print(f"Termine : {sent} lignes envoyees sur le topic '{args.topic}'.")


if __name__ == "__main__":
    main()
