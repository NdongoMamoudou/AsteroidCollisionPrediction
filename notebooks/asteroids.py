from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Création de la session Spark
spark = SparkSession.builder.appName("AsteroidsApp").getOrCreate()

# Lecture du CSV
df = spark.read.option("header", True).csv("hdfs://namenode:9000/user/hdfs/kafka_data/celestial_bodies.csv")

# Conversion des colonnes en types numériques
df = df.withColumn("mass", col("mass").cast("double")) \
       .withColumn("size", col("size").cast("double")) \
       .withColumn("x", col("x").cast("double")) \
       .withColumn("y", col("y").cast("double")) \
       .withColumn("z", col("z").cast("double")) \
       .withColumn("vx", col("vx").cast("double")) \
       .withColumn("vy", col("vy").cast("double")) \
       .withColumn("vz", col("vz").cast("double"))

# Afficher les 5 premières lignes après la conversion
df.show(5)

# Afficher le schéma mis à jour
df.printSchema()



