# batch-ca

Job MapReduce : chiffre d'affaires net par pays et par mois, à partir de `../data/online_retail.tsv`.

- `com.projetfinal.batch.CAMapper` : parse chaque ligne du TSV (`InvoiceNo StockCode Description Quantity InvoiceDate UnitPrice CustomerID Country`), ignore l'en-tête, émet `"Pays\tAAAA-MM"` → `Quantity * UnitPrice`.
- `com.projetfinal.batch.CAReducer` : somme les montants par clé (utilisé aussi comme combiner).
- `com.projetfinal.batch.CAParPaysMois` : driver.

## Exécution en local

```
mvn package
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
java -Djava.library.path="$HADOOP_HOME/bin" -cp "target/classes;$(cat cp.txt)" \
  com.projetfinal.batch.CAParPaysMois src/main/resources/input/sample.tsv src/main/resources/output
```

(le dossier de sortie ne doit pas déjà exister). Voir [`../../tp1-wordcount/README.md`](../../tp1-wordcount/README.md) pour les prérequis JDK 8 / `winutils.exe` / `HADOOP_HOME`.

## Exécution sur le cluster Docker

```
docker cp ../data/online_retail.tsv hadoop-master:/root/online_retail.tsv
docker cp target/batch-ca-1.jar hadoop-master:/root/batch-ca-1.jar

docker exec hadoop-master bash -lc "
  hadoop fs -mkdir -p input-ca &&
  hadoop fs -put -f /root/online_retail.tsv input-ca/online_retail.tsv &&
  hadoop fs -rm -r -f output-ca &&
  hadoop jar /root/batch-ca-1.jar com.projetfinal.batch.CAParPaysMois input-ca output-ca &&
  hadoop fs -cat output-ca/part-r-00000
"
```

Validé : 541 909 lignes de données → 314 lignes (pays × mois) en sortie, identique en local et sur le cluster.
