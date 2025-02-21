from pyspark.sql import SparkSession

# Création de la session Spark
spark = SparkSession.builder \
    .appName("CelestialBodiesApp") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Essai de lecture du fichier CSV depuis HDFS
file_path = "hdfs://namenode:9000/user/hdfs/kafka_data/celestial_bodies.csv"

# Vérification si le fichier existe sur HDFS
try:
    df = spark.read.option("header", "true").csv(file_path)

    # Afficher les 5 premières lignes
    df.show(5)

    # Afficher le schéma du DataFrame
    df.printSchema()

except Exception as e:
    print(f"Erreur lors de la lecture du fichier HDFS: {e}")

# Arrêter la session Spark
spark.stop()
