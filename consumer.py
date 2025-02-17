from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.appName("AsteroidCollisionPrediction").getOrCreate()

# Lire les données depuis Kafka en temps réel
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "asteroid_data").load()

# Décode les valeurs Kafka en format JSON
df = df.selectExpr("CAST(value AS STRING)")

# Convertir la colonne JSON en un DataFrame structuré
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, MapType

# Schéma des données
schema = StructType([
    StructField("planets", MapType(StringType(), StructType([  # Liste des planètes (ici Terre)
        StructField("planet", StringType()),
        StructField("position", MapType(StringType(), FloatType()))  # Position de la planète
    ]))),
    StructField("asteroids", MapType(StringType(), StructType([  # Liste des astéroïdes
        StructField("id", StringType()),
        StructField("position", MapType(StringType(), FloatType())),  # Position de l'astéroïde
        StructField("velocity", MapType(StringType(), FloatType())),  # Vitesse de l'astéroïde
        StructField("size", FloatType()),  # Taille de l'astéroïde
        StructField("mass", FloatType())   # Masse de l'astéroïde
    ])))
])

# Appliquer le schéma sur les données JSON
df = df.select(from_json(col("value"), schema).alias("data"))

# Sélectionner les colonnes pour les astéroïdes
df_asteroids = df.selectExpr("explode(data.asteroids) AS asteroid").select("asteroid.id", "asteroid.position", "asteroid.size", "asteroid.mass")

# Afficher les résultats dans la console en temps réel
query = df_asteroids.writeStream.outputMode("append").format("console").start()

# Attendre que le traitement en streaming se termine
query.awaitTermination()
