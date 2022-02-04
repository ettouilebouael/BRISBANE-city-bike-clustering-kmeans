from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import configparser
import folium

# 1 - Instancier le client Spark Session
spark = SparkSession.builder.master('local').appName('BRISBANE-City-bike-clustering-kmeans').getOrCreate()

# 2- Créer un fichier properties.conf contenant les informations relatives à vos paramètres du programme en dur.
config = configparser.ConfigParser()
config.read('properties.conf')
path_to_input_data = config['Bristol-City-bike']['Input-data']
path_to_output_data = config['Bristol-City-bike']['Output-data']
num_partition_kmeans = int(config['Bristol-City-bike']['Kmeans-level'])

# 3- Importer le json avec spark :  bristol = spark.read.json. en utilisant la variable path-to-input-data
bristol = spark.read.json(path_to_input_data)

# 4- créer un nouveau data frame Kmeans-df contenant seulement les variables latitude et longitude. 
kmeans_df = bristol.select("latitude","longitude")

# 5- k means.
features = ('longitude','latitude')
kmeans = KMeans().setK(num_partition_kmeans).setSeed(42)
assembler = VectorAssembler(inputCols = features, outputCol = "features")
dataset = assembler.transform(kmeans_df)
model = kmeans.fit(dataset)
fitted = model.transform(dataset)

# 6- quels sont les noms des colonnes de fitted ? vérifier qu’il s’agit de longitude, latitude, features, predictions.
print(f"Les nom des colonnes de fitted : {fitted.columns}")

# 7- Déterminer les longitudes et latitudes moyennes pour chaque groupe en utilisant spark DSL et SQL. comparer les résultats
## Méthode DSL
moyennes_par_cluster_DSL = fitted.groupBy(fitted.prediction).agg(mean('latitude').alias('Latitude_moyenne'),
                                                    mean('longitude').alias('Longitude_moyenne')).orderBy('prediction')

print("Les longitudes et latitudes moyennes pour chaque groupe par DSL")
moyennes_par_cluster_DSL.show()

## Méthode SQL
fitted.createOrReplaceTempView("fittedSQL")
moyennes_par_cluster_SQL = spark.sql("""
    select prediction,
           mean(latitude) as Latitude_moyenne,
           mean(longitude) as Longitude_moyenne
    from fittedSQL
    group by prediction
    order by prediction
""")

print("Les longitudes et latitudes moyennes pour chaque groupe par SQL :")
moyennes_par_cluster_SQL.show()

# 8-  Exporter la data frame fitted après élimination de la colonne  features, dans le répertoire
fitted.drop('features').write.format("csv").mode("overwrite").save(path_to_output_data, header = 'true')


# 9-  Faire une visualisation dans une map avec le package leaflet  (correction) (La ville est en Australie et s’appelle BRISBANE et non pas BRISTOLE) (Python)

data = fitted.toPandas()
data['name'] = bristol.select("name").toPandas()

# Définition des coordonnées pour centrer la carte
meanlat = data['latitude'].mean()
meanlong = data['longitude'].mean()

# Colorier les vélos en fonction de leur classe
def color(prediction):
    if prediction == 0:
        col = 'green'
    elif prediction == 1:
        col = 'blue'
    else:
        col = 'orange'
    return col

map_velo = folium.Map(location = [meanlat, meanlong], zoom_start = 14)

for latitude, longitude, name, prediction in zip(data['latitude'], data['longitude'], data['name'], data['prediction']):
    folium.Marker(location=[latitude, longitude], popup=name,
                  icon=folium.Icon(color=color(prediction),
                                   icon_color='yellow', icon='bicycle', prefix='fa')).add_to(map_velo)

## Enregistrer la carte
output_map = f"{path_to_output_data}carte_velo_brisbane.html"
map_velo.save(output_map)

### Arrêter la session SPARK
spark.stop()
