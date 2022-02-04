# BRISBANE-city-bike-clustering-kmeans
L'objectif principal de ce projet est de proposer un k-means clustering de Bristol City Bike en fonction de l'emplacement des stations vélos en utilisant spark.


> Prerequisites
- Python 3.6+ 
- Java 8
- Spark 3.2.0
- PySpark 3.2.0
- Folium 0.12.1
- Pandas 1.2.4
<br>

## Comment exécuter le code sur linux ?
```sh
# Cloner le repertoire GIT
git clone https://github.com/ettouilebouael/BRISBANE-city-bike-clustering-kmeans.git

# Accèder au dossier du code
cd BRISBANE-city-bike-clustering-kmeans

# Executer le script
spark-submit run.py
```
<br>

## Données
Le fichier BRISBANE-city-bike.json  contient des informations concernant l’emplacement de chaque vélo. Le jeu de données contient le variables suivantes :
- Adresse
- Latitude
- Longitude
- name
- number


## Résultas
### DSL

|prediction|   Latitude_moyenne| Longitude_moyenne|
|:--------:|------------------:|-----------------:|
|         0| -27.47285783582089|153.02444714925372|
|         1|-27.460720829787235|153.04137646808513|
|         2|-27.482543657142866|153.00442045714286|
---------------------------------------------------


### SQL
|prediction|   Latitude_moyenne| Longitude_moyenne|
|:---------|------------------:|-----------------:|
|         0| -27.47285783582089|153.02444714925372|
|         1|-27.460720829787235|153.04137646808513|
|         2|-27.482543657142866|153.00442045714286|
---------------------------------------------------



## Visualisation cartographique

Une version interactive de la carte : [lien](https://htmlpreview.github.io/?https://raw.githubusercontent.com/ettouilebouael/BRISBANE-city-bike-clustering-kmeans/main/exported/carte_velo_brisbane.html)
``````````````````
