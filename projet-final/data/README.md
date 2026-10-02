# Dataset : Online Retail

Téléchargé depuis UCI Machine Learning Repository (source officielle, sans authentification) :
https://archive.ics.uci.edu/static/public/352/online+retail.zip

```
curl -L -o online_retail.zip "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
unzip online_retail.zip   # -> "Online Retail.xlsx"
```

Fichiers présents ici :
- `Online Retail.xlsx` : fichier original (Excel).
- `online_retail.csv` : converti depuis le xlsx (voir `../scripts/convert_and_explore.py`), plus pratique pour Hadoop/HDFS et pour le futur producteur de streaming.
- `online_retail.tsv` : même contenu, séparé par des tabulations. À privilégier pour les jobs MapReduce (comme dans TP1, parsing naïf par `split("\t")`) car certains champs `Description` contiennent des virgules — le CSV les entoure de guillemets, ce qui casserait un simple `split(",")` en Java. Aucun champ du dataset ne contient de tabulation (vérifié), donc l'export TSV est fiable.

Ces fichiers sont volumineux (~20-25 Mo chacun) — à exclure du contrôle de version (`.gitignore`) si le dépôt est versionné avec git.

## Constat après exploration

- **541 909 lignes**, 8 colonnes : `InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country`.
- Période : 2010-12-01 → 2011-12-09.
- 38 pays, très dominé par le Royaume-Uni (495 478 lignes / 91%) — le reste (Allemagne, France, Irlande/EIRE, Espagne...) est bien plus petit en volume : à garder en tête pour les agrégats par pays (UK va écraser les autres sur un graphe non normalisé).
- **10 624 lignes à quantité négative** = des retours, à traiter explicitement (les exclure ou les compter à part selon l'analyse).
- `CustomerID` a **135 080 valeurs manquantes** (~25%) — commandes sans client identifié (achats invités ?). `Description` a 1 454 valeurs manquantes.
- Pas de colonne "coût"/normalisation : le montant d'une ligne = `Quantity * UnitPrice`.
