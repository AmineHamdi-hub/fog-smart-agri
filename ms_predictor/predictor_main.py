import paho.mqtt.client as mqtt
import os
import json
import random # Pour simuler un modèle sans dépendances lourdes

# Configuration
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-broker")
TOPIC_INPUT = "/sensors/field1/meteo"
TOPIC_OUTPUT = "/predictor/field1/evap_rate"

# Simuler des coefficients de modèle (en réalité, ce serait un modèle chargé)
# EvapRate = c1*Temp + c2*Wind + c3
COEFF_TEMP = 0.05
COEFF_WIND = 0.15
INTERCEPT = 0.1

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Predictor MS : Connecté, écoute des données météo...")
        client.subscribe(TOPIC_INPUT)
    else:
        print(f"Predictor MS : Échec de la connexion, code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        
        Temp = payload.get("temp", 0)
        Wind = payload.get("wind_speed", 0)
        
        # --- EXÉCUTION DU MODÈLE AI (Inférérence) ---
        # Calcul basé sur la formule simplifiée
        EvapRate = (COEFF_TEMP * Temp) + (COEFF_WIND * Wind) + INTERCEPT
        
        # Ajout d'une petite variation aléatoire pour le réalisme
        EvapRate += random.uniform(-0.05, 0.05)
        
        # S'assurer que le taux est positif
        EvapRate = max(0.0, EvapRate)
        
        # --- PUBLICATION du résultat vers le Decision Engine ---
        output_payload = {"time": time.time(), "value": round(EvapRate, 3)}
        client.publish(TOPIC_OUTPUT, json.dumps(output_payload))
        
        print(f"[INFÉRENCE] T:{Temp:.1f}, W:{Wind:.1f} -> EvapRate Publié: {EvapRate:.3f}")
        
    except Exception as e:
        print(f"Erreur de traitement du message: {e}")

if __name__ == "__main__":
    client = mqtt.Client(client_id="predictor-ms")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()