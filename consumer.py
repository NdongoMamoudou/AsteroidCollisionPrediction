from kafka import KafkaConsumer
import json
import csv
import os
from hdfs import InsecureClient
from datetime import datetime

# Configuration Kafka
KAFKA_BROKER = "kafka:9092"  # Nom du broker Kafka
TOPIC = "AsteroidesTopic"     # Nom du topic Kafka
CSV_FILE = "/tmp/celestial_bodies.csv"  # Fichier CSV local
HDFS_DIR = "/user/hdfs/kafka_data"  # Répertoire HDFS où envoyer le fichier

# URL du NameNode 
HDFS_URL = 'http://namenode:9870'  # Nom du service et port HDFS dans Docker

# Créer un client HDFS
client = InsecureClient(HDFS_URL, user='hdfs')

# Création du Consumer Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id="group_celestial_bodies"
)

# Ouvrir le fichier CSV une seule fois
with open(CSV_FILE, mode='w', newline='') as file:
    writer = csv.writer(file)
    # En-tête qui couvre à la fois les astéroïdes et les planètes
    writer.writerow(["id", "type", "mass", "size", "x", "y", "z", "vx", "vy", "vz"])  
    print("En attente de messages Kafka...")

    count = 0
    for message in consumer:
        data = message.value

        # Vérifier si le type est "asteroid" ou "planet"
        if data["type"] == "asteroid" or data["type"] == "planet":
            writer.writerow([ 
                data["id"], 
                data["type"], 
                data["mass"], 
                data["size"],
                data["position"]["x"], 
                data["position"]["y"], 
                data["position"]["z"],
                data["velocity"]["vx"] if "velocity" in data else 0.0,  # Valeur par défaut pour la vitesse
                data["velocity"]["vy"] if "velocity" in data else 0.0,  # Valeur par défaut pour la vitesse
                data["velocity"]["vz"] if "velocity" in data else 0.0   # Valeur par défaut pour la vitesse
            ])
            count += 1
            print(f"✅ {data['type'].capitalize()} reçu et enregistré : {data['id']}")

        # Après avoir reçu 10 messages, envoyer sur HDFS
        if count >= 10:
            print(f"Envoi du fichier {CSV_FILE} vers HDFS...")

            # Créer le répertoire HDFS si nécessaire
            try:
                if not client.status(HDFS_DIR, strict=False):  # Vérifie si le répertoire existe
                    client.makedirs(HDFS_DIR)  # Créer le répertoire si nécessaire
            except Exception as e:
                print(f"Erreur lors de la création du répertoire HDFS: {e}")

            # Transférer le fichier CSV vers HDFS
            try:
                client.upload(HDFS_DIR, CSV_FILE, overwrite=True)
                print("✅ Données mises à jour sur HDFS")
            except Exception as e:
                print(f"Erreur lors du transfert du fichier vers HDFS: {e}")

            # Réinitialiser le compteur sans fermer le fichier
            count = 0
