# README.md — PageRank with Spark

##  1. Introduction

Ce projet a été réalisé dans le cadre du TP sur l'implémentation de l'algorithme PageRank à grande échelle en utilisant Apache Spark sur un cluster Google Cloud Dataproc.

L'objectif principal est :

* d'exécuter PageRank sur deux structures de données Spark : RDD et DataFrames
* d'évaluer l'impact du nombre de workers (2, 4, 6 workers) sur les temps d'exécution
* d'étudier le rôle du nombre de partitions et son influence sur les performances
* de comparer les résultats obtenus sur un échantillon (10%) puis sur le dataset complet
* de générer et analyser les Top 10 pages selon le score PageRank.


##  2. Méthodes & Technologies utilisées

### 2.1 PageRank

Implémentation classique de l'algorithme via :

* transformation des graphes avec RDD
* utilisation de DataFrames optimisés (joins, groupBy, aggregations)
* itérations successives avec facteur d'amortissement (damping factor)

### 2.2 Apache Spark

Utilisation des modes :

* RDD  
* DataFrame  

### 2.3 Infrastructure

Exécution sur Google Cloud Dataproc.

Configuration utilisée :

* master : n1-standard-2
* workers : 2, puis 4, puis 6
* region : europe-west1
* image version : 2.1-debian

Le dataset est stocké sur Google Cloud Storage (GCS).

### 2.4 Gestion des partitions

Pour chaque configuration (2w, 4w, 6w), le nombre de partitions optimal a été déterminé grâce à un test sur un échantillon (10% du dataset).

Pour chaque script, la valeur retenue est celle qui a donné le temps d'exécution le plus faible.



## 3. Architecture du dépôt Git

```
project/
│
│
├── data/                      # les pages wikipedia (données compressées)

├── scripts/                   # tous les scripts Spark exécutés sur Dataproc

│   ├── preprocess_sample.py   # lecture du fichier compressé et extraction des entités (dst et src) + réduire la taille
│   ├── preprocess_full.py     # lecture du fichier compressé et extraction des entités (dst et src) sur toutes les données

│   ├── pagerank_sample_df_2w.py
│   ├── pagerank_full_df_2w.py
│   ├── pagerank_sample_df_4w.py
│   ├── pagerank_full_df_4w.py
│   ├── pagerank_sample_df_6w.py
│   ├── pagerank_full_df_6w.py
│   ├── pagerank_sample_rdd_2w.py
│   ├── pagerank_full_rdd_2w.py
│   ├── pagerank_sample_rdd_4w.py
│   ├── pagerank_full_rdd_4w.py
│   ├── pagerank_sample_rdd_6w.py
│   ├── pagerank_full_rdd_6w.py
│
├── results/                   # résultats générés (Top10 + Parquet)
│   ├── top10_full_df_2w.parquet
│   ├── top10_full_df_4w.parquet
│   ├── top10_full_df_6w.parquet
│   ├── top10_full_rdd_2w.parquet
│   ├── top10_full_rdd_4w.parquet
│   ├── top10_full_rdd_6w.parquet
│   └── res.ipynb               # notebook qui lit et affiche les résultats
│
│
├── requirements.txt
│
└── README.md
```



## 4. Lecture des résultats

Le dossier `results/` contient un notebook `res.ipynb` permettant de lire facilement les fichiers Parquet générés, par exemple :

Tous les résultats (DF et RDD) suivent ce même format.

###  Page la plus importante (tous essais confondus)

Dans tous les cas, la page ayant obtenu le score PageRank le plus élevé est :
"http://dbpedia.org/resource/Category:Living_people"



## 5. Résultats : temps d'exécution

### 5.1 DataFrame – Full dataset

|     Config         |      Temps      |
 
| DF – 2 workers | 56 min 7 s |

| DF – 4 workers | 3 h 36 min |

| DF – 6 workers | 1 h 10 min |

### 5.2 RDD – Full dataset

|      Config          |    Temps       |
 
| RDD – 2 workers | 1 h 29 min  |

| RDD – 4 workers | 55 min 58 s |

| RDD – 6 workers | 27 min 13 s |



## 6. Analyse des performances

### 1. Les RDD sont plus rapides que les DataFrames

Contrairement à ce que l'on pourrait attendre, les DataFrames ont été beaucoup plus lents, surtout avec 4 workers.

Causes possibles :

* joins trop lourds et mal distribués
* shuffle massif 

➡️ Résultat : les RDD sont plus performants sur ce TP.


###  2. L'augmentation du nombre de workers ne garantit PAS une amélioration

Nous obtenons des résultats non linéaires :

* DF : meilleure performance en 2 workers qu'en 4 et 6
* RDD : accélération nette de 2 → 4 → 6 workers

Explication :

* Le répartissement des partitions et le shuffle ne scale pas toujours bien
* Trop de workers peut provoquer plus de communication que de calcul utile


### 3. Importance du nombre de partitions

Avant chaque exécution, un test a été effectué sur un échantillon (10%) pour déterminer :

* le nombre de partitions optimal
* celui donnant le meilleur temps

Ce nombre a été fixé dans les scripts finaux.


##  7. Conclusion

Ce projet montre que :

* l'optimisation Spark dépend fortement de la nature des données
* les RDD peuvent être plus performants que les DataFrames dans des scénarios très orientés graphes
* le scaling horizontal (ajouter des workers) doit être mesuré soigneusement
* choisir le bon nombre de partitions est essentiel
* Dataproc facilite l'exécution distribuée mais nécessite une configuration rigoureuse.

