# Streaming + stockage + dashboard

Brique streaming du projet, construite sur les décisions techniques prises :
**Kafka + Spark Streaming** pour le traitement en direct, **Postgres** pour le
stockage des résultats (batch + streaming), **Grafana** pour le dashboard.

## Composants

```
producer.py  ──▶  Kafka (topic "online-retail")  ──▶  streaming_job.py (Spark)  ──▶  Postgres  ──▶  Grafana
                                                                                          ▲
                                              load_batch_results.py (sortie batch-ca) ────┘
```

- **`docker-compose.yml`** : Kafka (image officielle `apache/kafka`, mode KRaft, un seul noeud — pas besoin de Zookeeper), Postgres 16, Grafana. Réseau Docker dédié (`retail-streaming`), séparé du réseau `hadoop` du TP1/batch-ca (aucune brique streaming n'a besoin de parler au cluster Hadoop).
- **`producer.py`** : lit `../data/online_retail.tsv`, trie par `InvoiceDate`, envoie chaque ligne en JSON sur le topic Kafka `online-retail`, avec un délai simulé compressé (`--speedup`, défaut 3600 = 1h simulée → 1s réelle, plafonné à `--max-sleep` secondes entre deux lignes).
- **`streaming_job.py`** : job PySpark Structured Streaming (Spark 3.5.3, compatible Java 8) qui consomme le topic et écrit dans Postgres via `foreachBatch` (voir choix de conception ci-dessous).
- **`load_batch_results.py`** : charge la sortie du job MapReduce `batch-ca` (`pays\tmois\tca`) dans la table `batch_ca_pays_mois`, pour que le dashboard combine vue batch (historique) et vue streaming (temps réel).
- **`sql/schema.sql`** : schéma Postgres, appliqué automatiquement au premier démarrage du conteneur (`docker-entrypoint-initdb.d`).
- **`grafana/`** : provisioning automatique de la datasource Postgres et du dashboard (`online-retail.json`), donc rien à configurer manuellement dans l'UI Grafana.

## Choix de conception : le fenêtrage streaming

Spark Structured Streaming ne supporte pas les fonctions de classement
(`row_number`, etc.) directement sur un DataFrame streaming — seulement sur un
DataFrame statique. Pour rester simple et fiable, `streaming_job.py` utilise
`foreachBatch` : chaque micro-batch (déclenché toutes les `TRIGGER_SECONDS` =
10s réelles) est traité comme un DataFrame statique, sur lequel on calcule
librement agrégats et classements avant d'écrire dans Postgres.

Une "fenêtre" dans `streaming_top_produits` correspond donc à un micro-batch
Spark (10s réelles), pas à une fenêtre sur le temps événementiel simulé du
dataset. Avec la compression du producteur (`--speedup`), 10s réelles
représentent plusieurs heures de données simulées — ce qui donne des fenêtres
avec suffisamment de lignes pour être pertinentes, tout en gardant un design
simple. `streaming_ca_cumule` et `streaming_ca_pays` sont de vrais cumuls
(incrémentés à chaque micro-batch, pas remis à zéro).

PySpark est fixé en version 3.5.3 (et non la dernière 4.x) car Spark 4
nécessite Java 17, alors que Java 8 reste nécessaire pour Hadoop 2.7.2
(TP1 + `batch-ca`) sur cette même machine ; 3.5.3 fonctionne avec le JDK 8
déjà en place (`JAVA_HOME`/`HADOOP_HOME` du TP1, réutilisés tels quels).

## Lancer la stack

```bash
cd streaming
docker compose up -d              # démarre Kafka, Postgres, Grafana
pip install kafka-python "pyspark==3.5.3" psycopg2-binary

# charger les résultats batch existants dans Postgres
python load_batch_results.py

# terminal 1 : producteur (rejoue le dataset vers Kafka)
python producer.py --speedup 3600 --max-sleep 1.0

# terminal 2 : job Spark Streaming
python streaming_job.py
```

Dashboard Grafana : http://localhost:3000 (admin/admin, ou accès anonyme
lecture seule activé). Namenode/YARN du cluster Hadoop (TP1 + batch-ca)
restent sur leurs ports habituels (`40070`, `8088`) — stack indépendante.

## Statut

Validé de bout en bout (producteur → Kafka → Spark Streaming → Postgres →
Grafana) :
- Stack Docker (`retail-kafka`, `retail-postgres`, `retail-grafana`) démarrée
  et saine, topic `online-retail` créé.
- `producer.py` a rejoué le dataset complet (541 909 lignes) vers Kafka.
- `streaming_job.py` (PySpark 3.5.3 + connecteur `spark-sql-kafka-0-10`) a
  consommé le flux sur 253 micro-batches et écrit dans Postgres : CA cumulé
  net = 9 849 844 (546 909 lignes, y compris un premier essai de 5 000 lignes
  cumulé avant le run complet), Royaume-Uni très dominant (≈ 84 % du CA),
  cohérent avec le job batch (`batch-ca`).
- Grafana répond en bonne santé (`/api/health`), le datasource Postgres et le
  dashboard `online-retail-main` sont bien provisionnés automatiquement.
