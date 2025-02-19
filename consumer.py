from kafka import KafkaConsumer
import json
from hdfs import InsecureClient

# Configuration du consommateur Kafka
KAFKA_BROKER = "kafka:9092"
TOPIC = "AsteroidesTopic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="space-consumer-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Configuration de HDFS
HDFS_URL = "http://namenode:9870"
HDFS_DIR = "/data/asteroids/asteroids.json"
client = InsecureClient(HDFS_URL)

# Écriture dans HDFS
hdfs_file_path = HDFS_DIR  # Correction du chemin

print("📡 En attente de messages sur le topic :", TOPIC)

with client.write(hdfs_file_path, append=True) as writer:
    for message in consumer:
        data = message.value
        print(f"📩 Message brut reçu : {json.dumps(data, indent=2)}")  # Debug

        try:
            if "type" in data:
                if data["type"] == "planet":
                    writer.write(json.dumps(data) + "\n")
                    print(f"🌍 Planète reçue et enregistrée : {json.dumps(data, indent=2)}")
                elif data["type"] == "asteroid":
                    writer.write(json.dumps(data) + "\n")
                    print(f"☄️ Astéroïde reçu et enregistré : {json.dumps(data, indent=2)}")
            else:
                print("⚠️ Message reçu sans clé 'type', enregistré quand même.")
                writer.write(json.dumps(data) + "\n")
        except Exception as e:
            print(f"❌ Erreur lors du traitement du message : {e}")
