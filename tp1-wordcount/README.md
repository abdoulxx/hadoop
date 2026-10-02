# TP1 - Hadoop et MapReduce

Projet Maven du TP1 (voir `../tp1_hadoop_map_raduce.pdf` et `../CLAUDE.md`).

## Contenu

- `tn.insat.tp1.WordCount` (+ `TokenizerMapper` / `IntSumReducer`) : l'exemple WordCount du sujet.
  - Entrée de test : `src/main/resources/input/file.txt`.
- `tn.insat.tp1.TotalVentesParMagasin` (+ `SalesMapper` / `SalesReducer`) : l'exercice d'application — total des ventes par magasin à partir de `purchases.txt`.
  - Entrée de test : `src/main/resources/input-purchases/purchases_sample.txt` (échantillon local, colonnes séparées par des tabulations : `date temps magasin produit cout paiement`). Le vrai `purchases.txt` complet est fourni dans le conteneur `hadoop-master`.

## Environnement vérifié sur cette machine

- JDK : `C:\Program Files\Eclipse Adoptium\jdk-8.0.504.1-hotspot` (Temurin 8u504).
- Maven : `C:\Users\AS237666\Downloads\apache-maven-3.9.16-bin\apache-maven-3.9.16` (à déplacer si besoin, ex: `C:\tools\apache-maven-3.9.16`).
- Hadoop sur Windows a besoin de `winutils.exe`/`hadoop.dll` (absents par défaut) pour exécuter un job en local. Récupérés depuis https://github.com/cdarlint/winutils (dossier `hadoop-2.7.2/bin`) et placés dans `C:\hadoop\bin`.

**Variables d'environnement à définir (persistant, via Paramètres système → Variables d'environnement) :**

```
JAVA_HOME   = C:\Program Files\Eclipse Adoptium\jdk-8.0.504.1-hotspot
HADOOP_HOME = C:\hadoop
PATH        += %JAVA_HOME%\bin;%HADOOP_HOME%\bin;<chemin maven>\bin
```

WordCount et TotalVentesParMagasin ont été compilés et exécutés en local avec succès avec cette configuration (résultats identiques à ceux attendus par le sujet), **puis validés sur le vrai cluster Docker** (3 conteneurs, YARN) avec le `purchases.txt` réel (4 138 476 lignes, 103 magasins en sortie). Note : le port du namenode a dû être remappé sur l'hôte (`40070` au lieu de `50070`, plage réservée par Windows/Hyper-V) — voir `../CLAUDE.md`.

## Exécution en local (WordCount)

Configuration "Application" : Main class `tn.insat.tp1.WordCount`, arguments :
```
src/main/resources/input/file.txt src/main/resources/output
```
(le dossier de sortie ne doit pas déjà exister — le supprimer entre deux exécutions).

## Exécution en local (Total ventes par magasin)

Main class `tn.insat.tp1.TotalVentesParMagasin`, arguments :
```
src/main/resources/input-purchases/purchases_sample.txt src/main/resources/output-sales
```

## Build + exécution sur le cluster Docker

```
mvn package install
docker cp target/wordcount-1.jar hadoop-master:/root/wordcount-1.jar

# dans le conteneur hadoop-master :
hadoop jar wordcount-1.jar tn.insat.tp1.WordCount input output
hadoop jar wordcount-1.jar tn.insat.tp1.TotalVentesParMagasin input output-sales
hadoop fs -tail output-sales/part-r-00000
```

Voir `../CLAUDE.md` pour les commandes de mise en place du cluster Docker (pull de l'image, création du réseau, des 3 conteneurs, `start-hadoop.sh`).
