import os
import time
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone

DB_URL = "postgresql://postgres:postgres@timescaledb:5432/iot_metrics"
MODEL_PATH = "/home/jovyan/work/models/xgb_model.json"
ESTACION = "ESP32_01"
INTERVALO_SEG = 300

features = [
    'hour', 'minute', 'dayofweek',
    'temp_lag_1', 'temp_lag_2', 'temp_lag_3',
    'hum_lag_1', 'hum_lag_2', 'hum_lag_3',
    'iaq_lag_1', 'iaq_lag_2', 'iaq_lag_3',
    'pres_lag_1', 'pres_lag_2', 'pres_lag_3',
    'eco2_lag_1', 'eco2_lag_2', 'eco2_lag_3',
    'temp_rolling_3', 'temp_rolling_5'
]


def risk_level(temp):
    if temp <= 8 and temp >= 2:
        return "normal"
    elif temp > 8 and temp <= 10:
        return "warning"
    else:
        return "critical"


def predict_and_store(engine, model):
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=1)

    query = """
        SELECT window_start, avg_temperatura, avg_humedad, avg_iaq,
               avg_presion, avg_eco2
        FROM air_quality_metrics
        WHERE estacion = %s AND window_start >= %s AND avg_temperatura < 20
        ORDER BY window_start ASC
    """
    df = pd.read_sql(query, engine, params=(ESTACION, since))

    if len(df) < 10:
        print(f"Datos insuficientes: {len(df)} registros")
        return

    df = df.dropna(subset=['avg_temperatura']).copy()
    df['window_start'] = pd.to_datetime(df['window_start'])
    df = df.set_index('window_start').resample('5T').mean(numeric_only=True).reset_index()
    df = df.dropna(subset=['avg_temperatura'])

    if len(df) < 6:
        print(f"Datos insuficientes tras resample: {len(df)}")
        return

    df['ds'] = pd.to_datetime(df['window_start'])
    df['hour'] = df['ds'].dt.hour
    df['minute'] = df['ds'].dt.minute
    df['dayofweek'] = df['ds'].dt.dayofweek

    for lag in [1, 2, 3]:
        df[f'temp_lag_{lag}'] = df['avg_temperatura'].shift(lag)
        df[f'hum_lag_{lag}'] = df['avg_humedad'].shift(lag)
        df[f'iaq_lag_{lag}'] = df['avg_iaq'].shift(lag)
        df[f'pres_lag_{lag}'] = df['avg_presion'].shift(lag)
        df[f'eco2_lag_{lag}'] = df['avg_eco2'].shift(lag)

    df['temp_rolling_3'] = df['avg_temperatura'].rolling(3).mean()
    df['temp_rolling_5'] = df['avg_temperatura'].rolling(5).mean()

    df = df.dropna()
    if len(df) == 0:
        print("Sin datos con features completas")
        return

    X = df[features].iloc[-1:]

    predicted = model.predict(X)[0]
    real = df['avg_temperatura'].iloc[-1]
    pred_time = df['window_start'].iloc[-1] + timedelta(minutes=5)
    risk = risk_level(predicted)

    insert_sql = """
        INSERT INTO temperature_predictions (time, estacion, real_temp, predicted_temp, model_name, risk_level)
        VALUES (:t, :estacion, :real, :pred, :model, :risk)
    """
    with engine.begin() as conn:
        conn.execute(
            text(insert_sql),
            {"t": pred_time, "estacion": ESTACION, "real": float(real),
             "pred": float(predicted), "model": "xgboost", "risk": risk}
        )

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Real={real:.2f}°C | "
          f"Prediccion={predicted:.2f}°C | Riesgo={risk}")


def main():
    print("Cargando modelo XGBoost...")
    model = xgb.XGBRegressor()
    if not os.path.exists(MODEL_PATH):
        print(f"Modelo no encontrado en {MODEL_PATH}")
        return
    model.load_model(MODEL_PATH)
    print("Modelo cargado OK")

    engine = create_engine(DB_URL)

    print(f"Iniciando predicciones cada {INTERVALO_SEG}s para {ESTACION}")
    while True:
        try:
            predict_and_store(engine, model)
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(INTERVALO_SEG)


if __name__ == "__main__":
    main()
