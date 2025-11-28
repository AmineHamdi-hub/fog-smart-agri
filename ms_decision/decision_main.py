import paho.mqtt.client as mqtt
import os
import json
import time

# Configuration
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-broker")
HUMIDITY_CRIT = float(os.getenv("HUMIDITY_THRESHOLD_CRITICAL", 25.0))
HUMIDITY_OPT = float(os.getenv("HUMIDITY_THRESHOLD_OPTIMIZE", 35.0))
WIND_MAX = 10.0 # m/s
TOPICS = [
    ("/sensors/field1/soil/humidity", 0),
    ("/predictor/field1/evap_rate", 0), # Résultat du service Predictor
    ("/sensors/field1/meteo", 0) # Pour vérifier la vitesse du vent
]

# État du Fog (Mémoire locale)
state = {
    "humidity": 50.0,
    "evap_rate": 0.0,
    "wind_speed": 0.0,
    "irrigation_on": False
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Decision Engine : Connecté, abonnement aux sujets...")
        client.subscribe(TOPICS)
    else:
        print(f"Decision Engine : Échec de la connexion, code {rc}")

def on_message(client, userdata, msg):
    global state
    payload = json.loads(msg.payload.decode())

    if msg.topic.endswith("humidity"):
        state["humidity"] = payload.get("value")
    elif msg.topic.endswith("evap_rate"):
        state["evap_rate"] = payload.get("value")
    elif msg.topic.endswith("meteo"):
        state["wind_speed"] = payload.get("wind_speed")
    
    # --- Déclencher l'analyse critique à chaque réception ---
    take_decision(client)

def take_decision(client):
    H = state["humidity"]
    E = state["evap_rate"]
    W = state["wind_speed"]

    print(f"\n[EVAL] H:{H:.1f}%, Evap:{E:.2f}, Vent:{W:.1f} m/s")

    # Règle 3 : Blocage Météo (Priorité Maximale)
    if W > WIND_MAX:
        print("   [BLOCAGE R3] Vent trop fort. Irrigation interdite.")
        return # Fin de la décision

    # Règle 1 : Urgence Critique
    if H < HUMIDITY_CRIT and not state["irrigation_on"]:
        print("   [ACTION R1] HUMIDITÉ CRITIQUE ! DÉCLENCHEMENT IMMÉDIAT.")
        command = {"state": "ON", "duration_min": 15}
        client.publish("/actuators/field1/irrigation/command", json.dumps(command))
        state["irrigation_on"] = True
        return

    # Règle 2 : Optimisation Prédictive
    if HUMIDITY_CRIT <= H < HUMIDITY_OPT and E > 0.5 and not state["irrigation_on"]:
        print("   [ACTION R2] Prédiction d'évaporation élevée. Irrigation préventive.")
        command = {"state": "ON", "duration_min": 10}
        client.publish("/actuators/field1/irrigation/command", json.dumps(command))
        state["irrigation_on"] = True
        return

    # Règle de Fin : Arrêt
    if H >= HUMIDITY_OPT and state["irrigation_on"]:
        print("   [ACTION FIN] Humidité restaurée. Arrêt de l'irrigation.")
        command = {"state": "OFF"}
        client.publish("/actuators/field1/irrigation/command", json.dumps(command))
        state["irrigation_on"] = False

if __name__ == "__main__":
    client = mqtt.Client(client_id="decision-ms")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()