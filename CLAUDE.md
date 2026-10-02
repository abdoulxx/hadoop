# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## État actuel

Deux sous-projets distincts cohabitent dans ce dépôt :

- [`tp1-wordcount/`](tp1-wordcount/) — le TP1 : projet Maven avec `WordCount` (exemple du sujet) et `TotalVentesParMagasin` (exercice d'application). Validé de bout en bout : compilation + exécution locale (JDK 8 Temurin + Maven + `winutils.exe`/`hadoop.dll` pour Hadoop-sur-Windows), puis exécution réussie sur le cluster Docker à 3 noeuds avec le `purchases.txt` réel (4 138 476 lignes → 103 magasins). Voir son [README](tp1-wordcount/README.md) pour l'environnement exact.
- [`projet-final/`](projet-final/) — le projet de cours (architecture Big Data batch + streaming + dashboard). Source de données choisie : dataset UCI/Kaggle "Online Retail" (e-commerce UK), téléchargé et exploré (`projet-final/data/`, voir son [README](projet-final/data/README.md)). Premier job batch implémenté et validé (local + cluster Docker) : [`projet-final/batch-ca/`](projet-final/batch-ca/README.md) calcule le CA net par pays et par mois. Le reste de l'architecture (streaming, dashboard) reste un brouillon à valider en groupe — voir son [README](projet-final/README.md).

Les notes ci-dessous résument le sujet de TP1 (`tp1_hadoop_map_raduce.pdf`) pour référence rapide sans avoir à relire le PDF.

## Environnement cible (d'après le sujet)

- Apache Hadoop 2.7.2, Java 1.8, projet Maven, construit/exécuté depuis IntelliJ IDEA (ou tout autre IDE).
- Un cluster Hadoop à 3 noeuds (1 namenode `hadoop-master`, 2 datanodes `hadoop-slave1`/`hadoop-slave2`) lancé via Docker, avec l'image `liliasfaxi/spark-hadoop:hv-2.7.2`.

### Lancer le cluster Docker

Cluster déjà mis en place et vérifié sur cette machine (namenode + 2 datanodes sains, YARN démarré). Port 50070 remplacé par **40070** sur l'hôte : Windows/Hyper-V réserve dynamiquement la plage `50060-50159` (voir `netsh int ipv4 show excludedportrange protocol=tcp`), qui bloque le mapping Docker sur `50070`. Namenode UI donc sur http://localhost:40070 au lieu de http://localhost:50070.

```
docker pull liliasfaxi/spark-hadoop:hv-2.7.2
docker network create --driver=bridge hadoop

docker run -itd --net=hadoop -p 40070:50070 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
  --name hadoop-master --hostname hadoop-master liliasfaxi/spark-hadoop:hv-2.7.2

docker run -itd -p 8040:8042 --net=hadoop \
  --name hadoop-slave1 --hostname hadoop-slave1 liliasfaxi/spark-hadoop:hv-2.7.2

docker run -itd -p 8041:8042 --net=hadoop \
  --name hadoop-slave2 --hostname hadoop-slave2 liliasfaxi/spark-hadoop:hv-2.7.2

docker exec -it hadoop-master bash
./start-hadoop.sh   # démarre namenode/datanodes + les daemons yarn, à exécuter dans le contenaire master
```

Interfaces web une fois le cluster lancé : statut du namenode sur `http://localhost:40070` (voir remarque sur le port ci-dessus), resource manager YARN / suivi des jobs sur `http://localhost:8088`, node managers des esclaves sur `http://localhost:8040` et `http://localhost:8041`.

### Commandes HDFS utilisées dans ce TP

```
hadoop fs -mkdir -p input          # créer un répertoire dans HDFS (utiliser -mkdir -p /user/root d'abord en cas d'erreur)
hadoop fs -put purchases.txt input # uploader un fichier local dans HDFS
hadoop fs -ls input                # lister un répertoire
hadoop fs -tail input/purchases.txt
hadoop fs -cat file.txt
hadoop fs -get file.txt            # télécharger depuis HDFS
hadoop fs -mv file.txt newfile.txt
hadoop fs -rm newfile.txt
```

## Projet Maven prévu (WordCount MapReduce)

Le sujet guide la création d'un projet Maven avec :
- `GroupId: hadoop.mapreduce`, `ArtifactId: wordcount`, `Version: 1`
- Package Java `tn.insat.tp1` sous `src/main/java`, contenant `TokenizerMapper`, `IntSumReducer`, et une classe driver `WordCount` (classes Hadoop standard `Mapper`/`Reducer`/`Job`).
- Dépendances `pom.xml` : `org.apache.hadoop:hadoop-common`, `hadoop-mapreduce-client-core`, `hadoop-hdfs`, `hadoop-mapreduce-client-common`, toutes fixées en version `2.7.2`.

### Exécution en local

Arguments de programme pour la classe principale `WordCount` : `src/main/resources/input/file.txt src/main/resources/output` (fichier d'entrée, puis répertoire de sortie — ce dernier ne doit pas déjà exister).

### Build et exécution sur le cluster

```
mvn package install                                          # produit target/wordcount-1.jar
docker cp target/wordcount-1.jar hadoop-master:/root/wordcount-1.jar
# dans le contenaire hadoop-master :
hadoop jar wordcount-1.jar tn.insat.tp1.WordCount input output
hadoop fs -tail output/part-r-00000                           # consulter les résultats
```

Toujours tester le job MapReduce en local avant de le lancer sur le cluster.

## Exercice d'application mentionné dans le sujet

Écrire un job MapReduce calculant le total des ventes par magasin à partir de `purchases.txt`, dont les colonnes sont : `date temps magasin produit cout paiement`.

## Projet de cours (portée distincte, plus large)

Le sujet mentionne également un projet de session (en groupes de 3 à 4) consistant à construire une architecture Big Data combinant ingestion en streaming, traitement batch et dashboarding temps réel. La source de données et l'architecture cible restent des décisions ouvertes selon le sujet — à vérifier avec l'utilisateur/le support de cours avant de présumer une stack précise.
