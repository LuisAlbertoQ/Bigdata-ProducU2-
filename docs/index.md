# Pipeline IoT Big Data - Arquitectura Kappa

Monitoreo ambiental IoT en tiempo real con Kafka, Spark Streaming, TimescaleDB, Grafana y Machine Learning.

---

## Arquitectura

```
[ESP32 + BME680]  →  datos reales del sensor
        ↓
   [Supabase]     →  almacenamiento historico (REST API via httpx)
        ↓
[Python Producer] →  lee Supabase + simula 5 estaciones
        ↓
 [Apache Kafka]   →  topic: iot.air_quality.streaming (3 particiones)
        ↓
[Spark Streaming] →  ventana 1min / slide 30s / watermark 30s / trigger 10s
        ↓  (foreachBatch - outputMode update)
 [TimescaleDB]    →  sink de series de tiempo (hypertable)
        ↓
   [Grafana]      →  7 paneles de monitoreo BI (10s refresh)
        ↑
  [Jupyter Lab]   →  notebooks: exploracion, pipeline + 5 notebooks ML
        ↓              (GPU: NVIDIA RTX 4050 CUDA 13.3)
  [Modelos ML]    →  Prophet / ARIMA / XGBoost / LSTM
```

---

## Tecnologias

| Componente | Tecnologia |
|---|---|
| Sensor IoT | Bosch BME680 + ESP32 |
| Streaming | Apache Kafka 7.5.0 |
| Procesamiento | Apache Spark Structured Streaming 3.5.0 |
| Sink temporal | TimescaleDB PG15 |
| Visualizacion | Grafana 10.4.0 |
| Analisis | Jupyter Lab + PySpark |
| ML Prophet | Facebook Prophet |
| ML ARIMA | Statsmodels ARIMA(2,1,1) |
| ML XGBoost | XGBoost con GPU (device='cuda') |
| ML LSTM | TensorFlow con GPU |
| GPU | NVIDIA RTX 4050 (CUDA 13.3, cuDNN 9) |
| Infraestructura | Docker + Docker Compose |

---

## Resultados Machine Learning

| Modelo | RMSE | MAE | MAPE (%) | vs Baseline |
|--------|:----:|:---:|:--------:|:-----------:|
| **ARIMA (2,1,1)** | **0.0861** | **0.0720** | **0.62** | **−1% (gana)** |
| Baseline (lag-1) | 0.0870 | 0.0537 | 0.45 | — |
| Prophet | 0.1351 | 0.1162 | 0.96 | +55% |
| LSTM (seed=42) | 0.2056 | 0.1753 | 1.45 | +136% |
| XGBoost | 0.3170 | 0.2511 | 2.07 | +264% |

Los datos se agregaron a ventanas de **5 minutos** para reducir ruido y obtener variacion significativa (~55 registros). Split temporal 80/20.

---

## Metricas del Pipeline

| Metrica | Valor |
|---|---|
| Throughput | 5 eventos cada 10s (0.5 ev/s) |
| Latencia normal | ~0ms |
| Latencia con delay | ~5000ms (ESP32_05) |
| Particiones Kafka | 3 |
| Watermark | 30s |
| Trigger | 10s |

---

## Estructura del proyecto

```
pipeline-iot-bigdata/
├── docker-compose.yml              # 6 contenedores + GPU Jupyter
├── producer/
│   ├── producer.py                 # Productor Kafka (Supabase + simulacion)
│   └── requirements.txt
├── notebooks/
│   ├── 01_exploracion_supabase.ipynb
│   ├── 02_verificacion_kafka.ipynb
│   ├── 03_streaming_pipeline.ipynb
│   ├── 04_modelo_prophet.ipynb
│   ├── 05_modelo_arima.ipynb
│   ├── 06_modelo_xgboost.ipynb
│   ├── 07_modelo_lstm.ipynb
│   └── 08_comparacion_modelos.ipynb
├── grafana/dashboards/
│   └── iot_dashboard.json
├── timescaledb/
│   └── init.sql
└── INFORME_UNIDAD2.md
```

---

## Enlaces

- [Repositorio en GitHub](https://github.com/TU_USUARIO/TU_REPO)
- [Informe completo](INFORME_UNIDAD2.md)

---

*Proyecto academico de Big Data - Arquitectura Kappa aplicada a IoT con Machine Learning*
