from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, MapType, ArrayType

try:
    spark = SparkSession.builder \
        .appName("AsteroidCollisionPrediction") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
    print("Session Spark lancée avec succès")
except Exception as e:
    print(f"Erreur lors du démarrage de Spark: {e}")
    exit(1)

# Lire les données depuis Kafka en temps réel
try:
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "asteroid_data") \
        .load()
    print("Connexion à Kafka réussie")
except Exception as e:
    print(f"Erreur de connexion à Kafka: {e}")
    exit(1)

# Décode les valeurs Kafka en format JSON
df = df.selectExpr("CAST(value AS STRING)")

# Schéma des données ajusté pour le format spécifique
schema = StructType([
    StructField("planets", ArrayType(StructType([  
        StructField("planet", StringType()),
        StructField("position", MapType(StringType(), FloatType()))
    ]))),
    StructField("asteroids", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("position", MapType(StringType(), FloatType())),
        StructField("velocity", MapType(StringType(), FloatType())),
        StructField("size", FloatType()),
        StructField("mass", FloatType())
    ])))
])

# Appliquer le schéma sur les données JSON
df = df.select(from_json(col("value"), schema).alias("data"))

# Exploser les astéroïdes pour en faire des lignes distinctes
df_asteroids = df.select(explode(col("data.asteroids")).alias("asteroid")) \
    .select("asteroid.id", "asteroid.position", "asteroid.size", "asteroid.mass")

# Extraire la position des astéroïdes dans des colonnes distinctes (x, y, z)
df_asteroids = df_asteroids \
    .withColumn("x", col("asteroid.position.x")) \
    .withColumn("y", col("asteroid.position.y")) \
    .withColumn("z", col("asteroid.position.z")) \
    .drop("asteroid.position")  # Supprimer la colonne 'position' une fois que les coordonnées ont été extraites

# Sélectionner également les informations de la Terre (fixes)
df_planets = df.select(explode(col("data.planets")).alias("planet")) \
    .select("planet.planet", "planet.position") \
    .withColumn("x", col("planet.position.x")) \
    .withColumn("y", col("planet.position.y")) \
    .withColumn("z", col("planet.position.z")) \
    .drop("planet.position") \
    .withColumn("id", col("planet.planet"))  # Ajouter l'ID pour la Terre

# Pour éviter la duplication, on s'assure qu'aucune colonne "planet" n'est incluse
df_planets = df_planets.select("id", "x", "y", "z")

# Combiner les données des astéroïdes et de la Terre dans le même DataFrame
df_combined = df_planets.union(df_asteroids)

# Afficher les résultats dans la console en temps réel
query = df_combined.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
