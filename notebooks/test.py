import json

file_path = "D:/CourIPPSSI_5émeAnnée/AsteroidCollisionPrediction/notebooks/asteroids.json"

try:
    with open(file_path, 'r') as f:
        # Charger tout le fichier comme un tableau d'objets JSON
        data = [json.loads(line) for line in f]
    print(f"Le fichier JSON est valide avec {len(data)} éléments.")
except json.JSONDecodeError as e:
    print(f"Erreur de décodage JSON : {e}")
