"""
Charge la sortie du job batch (batch-ca) dans Postgres (table batch_ca_pays_mois),
pour que le dashboard Grafana puisse combiner vue batch (historique) et vue
streaming (temps reel).

Usage:
    python load_batch_results.py [chemin_vers_part-r-00000]

Par defaut, utilise la sortie locale deja validee dans batch-ca/.
"""

import sys
from pathlib import Path

import psycopg2

DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent.parent
    / "batch-ca" / "src" / "main" / "resources" / "output-full" / "part-r-00000"
)

PG_DSN = dict(host="localhost", port=5433, dbname="retail", user="retail", password="retail")


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT
    print(f"Lecture de {path} ...")

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            pays, mois, ca = line.split("\t")
            rows.append((pays, mois, float(ca)))
    print(f"{len(rows)} lignes pays x mois a charger.")

    conn = psycopg2.connect(**PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO batch_ca_pays_mois (pays, mois, ca)
                VALUES (%s, %s, %s)
                ON CONFLICT (pays, mois) DO UPDATE SET ca = EXCLUDED.ca
                """,
                rows,
            )
        conn.commit()
        print("Charge dans Postgres (table batch_ca_pays_mois).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
