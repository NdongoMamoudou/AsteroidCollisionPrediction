from kafka import KafkaProducer
import json
import random
import time

# Configuration du producteur Kafka
KAFKA_BROKER = "kafka:9092"
TOPIC = "AsteroidesTopic"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Coordonnées fixes de la planète
PLANET = {
    "type": "planet",
    "planet": "Earth",
    "position": {"x": 0.0, "y": 0.0, "z": 0.0}
}

# Envoi des données de la Terre une seule fois
producer.send(TOPIC, value=PLANET)
print(f"✅ Données de la planète envoyées : {json.dumps(PLANET, indent=2)}")

def generate_asteroid_data():
    asteroid_id = f"asteroid_{random.randint(1, 1000):03d}"
    position = {
        "x": round(random.uniform(-1e6, 1e6), 2),
        "y": round(random.uniform(-1e6, 1e6), 2),
        "z": round(random.uniform(-1e6, 1e6), 2)
    }
    velocity = {
        "vx": round(random.uniform(-50, 50), 2),
        "vy": round(random.uniform(-50, 50), 2),
        "vz": round(random.uniform(-50, 50), 2)
    }
    size = round(random.uniform(0.5, 10.0), 2)
    mass = round(random.uniform(1e10, 1e15), 2)
    
    return {
        "type": "asteroid",
        "id": asteroid_id,
        "position": position,
        "velocity": velocity,
        "size": size,
        "mass": mass
    }

# Limite du nombre d'astéroïdes générés
MAX_ASTEROIDS = 10000  
count = 0

while count < MAX_ASTEROIDS:
    batch = [generate_asteroid_data() for _ in range(10)]
    
    for data in batch:
        # Vérification que tous les messages contiennent "type"
        if "type" not in data:
            print("❌ Erreur : message sans clé 'type'", data)
        else:
            print(f"📤 Envoi de l'astéroïde : {json.dumps(data, indent=2)}")
            producer.send(TOPIC, value=data)
    
    count += len(batch)
    print(f"✅ Envoyé {len(batch)} astéroïdes, total : {count}")
    time.sleep(2)
