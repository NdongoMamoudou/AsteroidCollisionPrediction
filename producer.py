from kafka import KafkaProducer
import time
import random
import json

# Configuration de Kafka
kafka_broker = "kafka:9092"
topic = "asteroid_data"

# Créer un producer Kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=2000000000
)

# Fonction pour générer la Terre avec des coordonnées fixes
def generate_planets():
    return [
        {
            "planet": "Terre",
            "position": {
                "x": 0,  # Coordonnées fixes de la Terre
                "y": 0,
                "z": 0   
            }
        }
    ]

# Fonction pour générer un astéroïde aléatoire
def generate_asteroid(id):
    return {
        "id": id,
        "position": {
            "x": random.uniform(-1e5, 1e5),
            "y": random.uniform(-1e5, 1e5),
            "z": random.uniform(-1e5, 1e5)
        },
        "velocity": {
            "vx": random.uniform(-50, 50),
            "vy": random.uniform(-50, 50),
            "vz": random.uniform(-50, 50)
        },
        "size": random.uniform(1, 3),
        "mass": random.uniform(1e12, 3e12)
    }

# Fonction pour envoyer des planètes et des astéroïdes
def send_data():
    asteroid_id = 1  
    while True:
        # Générer les données pour la Terre
        planets_data = generate_planets()
        
        # Générer un certain nombre d'astéroïdes (par exemple, 5 astéroïdes)
        asteroids_data = [generate_asteroid(f"asteroid_{asteroid_id + i}") for i in range(5)]
        
        # Créer un message à envoyer à Kafka
        message = {
            "planets": planets_data,
            "asteroids": asteroids_data
        }
        
        # Envoyer les données au topic Kafka
        producer.send(topic, message)
        
        # Afficher ce qui est envoyé (utile pour déboguer)
        print(f"Envoyé au Kafka Topic '{topic}': {message}")
        
        # Incrémenter l'id des astéroïdes
        asteroid_id += 5
        
        # Attendre 1 seconde avant d'envoyer les prochaines données
        time.sleep(1)

if __name__ == "__main__":
    send_data()
