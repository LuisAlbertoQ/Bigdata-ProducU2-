import os
import json
import time
import random
import logging
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ffwjdoguzaqpafuwaeam.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "iot.air_quality.streaming"
DLQ_TOPIC = "iot.air_quality.streaming.dlq"

ESTACIONES = {
    "ESP32_01": {"temp_var": 0.0, "hum_var": 0.0, "iaq_var": 0, "delay": 0},
    "ESP32_02": {"temp_var": 0.5, "hum_var": 2.0, "iaq_var": 0, "delay": 0},
    "ESP32_03": {"temp_var": 1.2, "hum_var": 3.0, "iaq_var": 0, "delay": 0},
    "ESP32_04": {"temp_var": 0.3, "hum_var": 1.0, "iaq_var": 20, "delay": 0},
    "ESP32_05": {"temp_var": 0.2, "hum_var": 1.5, "iaq_var": 0, "delay": 5},
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    max_block_ms=5000,
)

supabase = None
use_mock_data = False

if SUPABASE_KEY:
    try:
        import httpx
        supabase = httpx.Client(
            base_url=SUPABASE_URL,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            }
        )
        resp = supabase.get("/rest/v1/grupo1_air_quality", params={"select": "id", "limit": 1})
        if resp.status_code == 200:
            logger.info("Conexion a Supabase establecida")
        else:
            logger.warning(f"Supabase respondio {resp.status_code}. Usando datos mock.")
            use_mock_data = True
    except Exception as e:
        logger.warning(f"Supabase no disponible ({e}). Usando datos mock.")
        use_mock_data = True
else:
    logger.warning("SUPABASE_KEY no configurada. Usando datos mock.")
    use_mock_data = True

MOCK_BASE = {
    "temperatura": 25.4,
    "humedad": 58.0,
    "presion": 1013.2,
    "altura": 3820,
    "gas": 120,
    "iaq": 42,
    "eco2": 620,
    "VOC": 0.45,
}

def fetch_real_data():
    if use_mock_data or supabase is None:
        return MOCK_BASE.copy()
    try:
        resp = supabase.get(
            "/rest/v1/grupo1_air_quality",
            params={"select": "*", "order": "created_at.desc", "limit": 1}
        )
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]
    except Exception as e:
        logger.error(f"Error fetching Supabase: {e}")
    return MOCK_BASE.copy()

def generate_event(base_data, estacion, config):
    timestamp = datetime.utcnow()

    def clamp(val, min_val, max_val):
        return max(min_val, min(max_val, val))

    event = {
        "estacion": estacion,
        "temperatura": round(clamp(base_data.get("temperatura", 25) + random.uniform(-config["temp_var"], config["temp_var"]), -10, 50), 2),
        "humedad": round(clamp(base_data.get("humedad", 50) + random.uniform(-config["hum_var"], config["hum_var"]), 0, 100), 2),
        "presion": round(base_data.get("presion", 1013) + random.uniform(-2, 2), 2),
        "altura": base_data.get("altura", 3820),
        "gas": max(0, base_data.get("gas", 120) + random.randint(-10, 10)),
        "iaq": max(0, min(500, base_data.get("iaq", 42) + config["iaq_var"] + random.randint(-5, 5))),
        "eco2": max(300, base_data.get("eco2", 620) + random.randint(-30, 30)),
        "VOC": round(max(0, base_data.get("VOC", 0.45) + random.uniform(-0.1, 0.1)), 3),
        "calidad_aire": classify_air(base_data.get("iaq", 42) + config["iaq_var"]),
        "created_at": timestamp.isoformat(),
        "event_timestamp": int(timestamp.timestamp() * 1000),
    }

    if config["delay"] > 0:
        delayed_time = timestamp.timestamp() - config["delay"]
        event["created_at"] = datetime.utcfromtimestamp(delayed_time).isoformat()
        event["event_timestamp"] = int(delayed_time * 1000)
        event["delayed"] = True

    return event

def classify_air(iaq):
    if iaq <= 50:
        return "BUENA"
    elif iaq <= 100:
        return "MODERADA"
    elif iaq <= 150:
        return "REGULAR"
    elif iaq <= 200:
        return "MALA"
    else:
        return "PELIGROSA"

def main():
    logger.info(f"Producer iniciado. Kafka: {KAFKA_BROKER}, Topic: {TOPIC}")
    logger.info(f"Estaciones configuradas: {list(ESTACIONES.keys())}")
    logger.info(f"Modo datos: {'MOCK' if use_mock_data else 'SUPABASE'}")

    base_data = fetch_real_data()
    logger.info(f"Datos base obtenidos: temp={base_data.get('temperatura')}, iaq={base_data.get('iaq')}")

    while True:
        try:
            if random.random() < 0.1 and not use_mock_data:
                fresh = fetch_real_data()
                if fresh:
                    base_data = fresh

            for estacion, config in ESTACIONES.items():
                event = generate_event(base_data, estacion, config)
                if event:
                    topic = DLQ_TOPIC if event.get("iaq", 0) > 500 else TOPIC
                    producer.send(topic, value=event)
                    logger.debug(f"Enviado a {topic}: {estacion} | IAQ: {event['iaq']}")

            producer.flush()
            logger.info(f"Batch enviado. {len(ESTACIONES)} eventos publicados.")
            time.sleep(int(os.getenv("PRODUCER_INTERVAL", "10")))

        except KeyboardInterrupt:
            logger.info("Producer detenido.")
            break
        except Exception as e:
            logger.error(f"Error en producer: {e}")
            time.sleep(3)

    producer.close()

if __name__ == "__main__":
    main()
