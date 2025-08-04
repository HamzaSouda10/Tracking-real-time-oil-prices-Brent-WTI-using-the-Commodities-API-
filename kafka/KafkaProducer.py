import requests
import json
import time
from kafka import KafkaProducer

# Configuration de l'API
API_KEY = "f8fd1e7a24215e3e4eefb1d4285e54b885075d54cbf8d3b116b2bdece58cd4ea"
URL = "https://api.oilpriceapi.com/v1/prices/latest"
HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type": "application/json"
}

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # ou 'kafka:9092' si tu exécutes ce code depuis un conteneur
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_oil_price():
    try:
        response = requests.get(URL, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data['data']
        else:
            print(f"Erreur API: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Erreur lors de l'appel API : {e}")
        return None

while True:
    oil_data = fetch_oil_price()
    if oil_data:
        producer.send('oil_prices', oil_data)
        print("✅ Donnée envoyée :", oil_data)
    time.sleep(60)  
