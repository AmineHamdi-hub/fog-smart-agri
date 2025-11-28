import paho.mqtt.client as mqtt
import os
import json

# Récupération de l'hôte MQTT à partir des variables d'environnement (docker-compose)
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-broker") 
MQTT_PORT = 1883
TOPICS = [
    ("/sensors/field1/soil/humidity", 0), 
    ("/sensors/field1/meteo", 0)
]

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Ingestor : Connecté au broker MQTT ({MQTT_HOST})")
        client.subscribe(TOPICS)
    else:
        print(f"Ingestor : Échec de la connexion, code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"\n[RECU] Sujet: {msg.topic}")
        print(f"Données brutes reçues : {payload}")
        
        # --- Logique d'ingestion/validation ---
        if msg.topic.endswith("humidity"):
            if 10.0 <= payload.get("value") <= 90.0:
                # Transmettre les données validées au moteur de décision (ici on le log juste)
                print(f"   [VALIDÉ] Humidité: {payload['value']}%")
            else:
                print(f"   [ERREUR] Humidité hors limites : {payload['value']}")
                
    except Exception as e:
        print(f"Erreur de traitement du message: {e}")

if __name__ == "__main__":
    client = mqtt.Client(client_id="ingestor-ms")
    client.on_connect = on_connect
    client.on_message = on_message
    
    # Utilisez le nom de service Docker pour la connexion
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    
    # Boucle de traitement des messages
    client.loop_forever()