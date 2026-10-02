# Projet de cours - Architecture Big Data (batch + streaming + dashboard)

Statut : projet solo (pas de travail de groupe pour ce projet). Batch et streaming implémentés et validés de bout en bout — voir ci-dessous.

## Source de données

**Online Retail** (transactions e-commerce UK, dataset Kaggle/UCI). Colonnes typiques :

```
InvoiceNo  StockCode  Description  Quantity  InvoiceDate  UnitPrice  CustomerID  Country
```

- Chaque ligne = une ligne de commande (un produit dans une facture).
- Champ `InvoiceDate` utilisable pour rejouer le fichier ligne par ligne, trié par date, comme flux "pseudo temps réel" (un producteur qui lit le CSV et envoie chaque ligne avec un léger délai).
- Thème proche du TP1 (`purchases.txt`) : facilite la réutilisation des jobs MapReduce déjà écrits dans [`../tp1-wordcount`](../tp1-wordcount) comme point de départ pour la partie batch.

Le fichier CSV n'est pas inclus ici (dataset externe, à télécharger manuellement — voir [`data/README.md`](data/README.md)).

## Batch : `batch-ca/` (premier job, déjà implémenté et validé)

Module Maven [`batch-ca/`](batch-ca/) : job MapReduce calculant le **chiffre d'affaires net par pays et par mois** (`Quantity * UnitPrice`, retours inclus donc net) à partir de `data/online_retail.tsv`. Réutilise le pattern de [`../tp1-wordcount`](../tp1-wordcount) (`CAMapper`/`CAReducer`/`CAParPaysMois` dans le package `com.projetfinal.batch`).

Validé de bout en bout :
- **Local** : sur un échantillon (`src/main/resources/input/sample.tsv`) puis sur le dataset complet (541 909 lignes → 314 lignes pays×mois), résultats vérifiés manuellement.
- **Cluster Docker** : même résultat sur le vrai cluster à 3 noeuds (`hadoop jar batch-ca-1.jar com.projetfinal.batch.CAParPaysMois input-ca output-ca`), Royaume-Uni très dominant (pic 2011-11 ≈ 1 282 806).

Sert de socle pour les prochains jobs batch (top produits, retours, clients actifs).

## Architecture cible

```
                 ┌─────────────────────┐
   CSV source ──▶│  Producteur replay  │──▶ file de streaming (ex: Kafka)
  (Online Retail)│  (simule le temps   │
                 │   réel ligne/ligne) │
                 └─────────────────────┘
                           │
              ┌────────────┴─────────────┐
              ▼                          ▼
     ┌─────────────────┐        ┌──────────────────┐
     │  HDFS (stockage │        │  Traitement       │
     │  brut, batch)   │        │  streaming        │
     └────────┬────────┘        │  (agrégats par    │
              ▼                 │  fenêtre glissante)│
     ┌─────────────────┐        └─────────┬─────────┘
     │  Jobs batch      │                  │
     │  (MapReduce/Spark│                  │
     │  - CA par pays   │                  │
     │  - top produits  │                  │
     │  - tendances)    │                  │
     └────────┬────────┘                  │
              └────────────┬───────────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Stockage résultats  │
                 │  (ex: Elasticsearch  │
                 │   ou base analytique)│
                 └─────────┬───────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Dashboard temps réel│
                 │  (ex: Kibana/Grafana │
                 │   ou app web dédiée) │
                 └─────────────────────┘
```

Pistes de traitements concrets :
- **Batch** : chiffre d'affaires par pays / par produit / par mois, clients les plus actifs, produits les plus retournés (quantités négatives dans le dataset = retours).
- **Streaming** : CA cumulé en direct, CA cumulé par pays, top produits par fenêtre (voir [`streaming/README.md`](streaming/README.md) pour le détail du choix de fenêtrage).
- **Dashboard** : vue temps réel (streaming) + vue historique (batch), filtrable par pays.

## Streaming + stockage + dashboard : `streaming/` (implémenté)

Module [`streaming/`](streaming/README.md) : Kafka (producteur qui rejoue `data/online_retail.tsv` trié par `InvoiceDate`), job Spark Structured Streaming qui consomme le topic et maintient des agrégats en direct, Postgres pour le stockage des résultats (batch + streaming), Grafana pour le dashboard. Voir son README pour le détail du fonctionnement, les choix techniques et les commandes de lancement.

## Décisions techniques

- **Streaming** : Kafka + Spark Streaming.
- **Dashboard** : Grafana.
- **Stockage des résultats agrégés** : base SQL (Postgres).

## Prochaines étapes

1. ~~Télécharger et explorer le dataset~~ (fait, voir [`data/README.md`](data/README.md)).
2. ~~Premier job batch (CA par pays/mois)~~ (fait, voir [`batch-ca/README.md`](batch-ca/README.md)).
3. ~~Producteur streaming + traitement Spark Streaming + stockage Postgres + dashboard Grafana~~ (fait, voir [`streaming/README.md`](streaming/README.md)).
4. Autres agrégats batch (top produits, retours, clients actifs) en réutilisant le pattern MapReduce de `batch-ca/`.
