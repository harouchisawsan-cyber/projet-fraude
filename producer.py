"""Rejeu du dataset Sparkov vers Kafka (flux fidèle, sans rééquilibrage).

Exemples (depuis la racine du projet, Kafka lancé par docker compose) :
    python producer.py                              # 10 000 lignes de fraudTest à 100 évts/s
    python producer.py --limit 0 --rate 1000        # tout le fichier à 1000 évts/s
    python producer.py --file data/fraudTrain.csv --limit 50000 --rate 0
"""
import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer

# colonnes du CSV -> noms des champs de l'événement JSON
COLUMNS = {
    "trans_num": "trans_num",
    "trans_date_trans_time": "event_time",   # on n'utilise PAS unix_time (décalée de 7 ans)
    "cc_num": "cc_num",
    "merchant": "merchant",
    "category": "category",
    "amt": "amount",
    "state": "state",
    "lat": "lat",
    "long": "lon",
    "merch_lat": "merch_lat",
    "merch_long": "merch_lon",
    "is_fraud": "is_fraud",                  # sert à évaluer, jamais à décider
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/fraudTest.csv")
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--rate", type=float, default=100,
                    help="événements par seconde (0 = le plus vite possible)")
    ap.add_argument("--limit", type=int, default=10000,
                    help="nombre maximum de lignes (0 = tout le fichier)")
    args = ap.parse_args()

    df = pd.read_csv(args.file, usecols=list(COLUMNS), dtype={"cc_num": str},
                     parse_dates=["trans_date_trans_time"]).rename(columns=COLUMNS)
    # le fichier n'est pas trié par date : tri stable (même seconde = ordre du fichier)
    df = df.sort_values("event_time", kind="stable").reset_index(drop=True)
    if args.limit > 0:
        df = df.head(args.limit)
    df["event_time"] = df["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["is_fraud"] = df["is_fraud"].astype(int)
    records = df.to_dict("records")

    print(f"{len(records)} événements, dont {int(df['is_fraud'].sum())} fraudes "
          f"({df['is_fraud'].mean():.2%}) -> topic '{args.topic}'")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,
    )

    start = time.perf_counter()
    for i, rec in enumerate(records, 1):
        # clé = cc_num : toutes les transactions d'une carte restent dans l'ordre
        producer.send(args.topic, key=rec["cc_num"], value=rec)
        if args.rate > 0 and i % 50 == 0:
            delay = start + i / args.rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
        if i % 5000 == 0:
            print(f"  {i} envoyés")

    producer.flush()
    print(f"Terminé en {time.perf_counter() - start:.1f} s")


if __name__ == "__main__":
    main()