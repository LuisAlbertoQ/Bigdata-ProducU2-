---
layout: default
title: IoT Environmental Monitoring Pipeline
---

# IoT Environmental Monitoring Pipeline

**Arquitectura Kappa** para monitoreo ambiental en tiempo real con Kafka, Spark Structured Streaming, TimescaleDB y Grafana. Incluye modelos de Machine Learning (Prophet, ARIMA, XGBoost, LSTM) para predicción de temperatura.

---

## Arquitectura del Pipeline

```
[ESP32 + BME680]  →  datos reales del sensor
        ↓
   [Supabase]     →  almacenamiento historico
        ↓
[Python Producer] →  lee Supabase + simula 5 estaciones
        ↓
 [Apache Kafka]   →  topic: iot.air_quality.streaming (3 particiones)
        ↓
[Spark Streaming] →  ventana 1min / slide 30s / watermark 30s
        ↓  (foreachBatch - outputMode update)
 [TimescaleDB]    →  sink de series de tiempo (hypertable)
        ↓
   [Grafana]      →  7 paneles de monitoreo BI
```

### Componentes

| Componente | Tecnología | Función |
|---|---|---|
| Fuente de datos | Bosch BME680 + ESP32 + Supabase | Captura de métricas ambientales |
| Ingesta | Apache Kafka 7.5.0 | Buffer distribuido, 3 particiones |
| Procesamiento | Spark Structured Streaming 3.5.0 | Agregaciones en tiempo real |
| Sink | TimescaleDB PG15 | Base de datos series temporales |
| Visualización | Grafana 10.4.0 | Dashboard de 7 paneles |
| Análisis | Jupyter Lab + PySpark | 8 notebooks de análisis y ML |

---

## Dashboard Grafana

> ⚠️ **Agrega aquí una captura de pantalla** de http://localhost:3000 mostrando los 7 paneles funcionando.

![Dashboard Grafana](assets/screenshots/grafana_dashboard.png)

### Paneles implementados

| Panel | Métrica | Descripción |
|---|---|---|
| 1 | Temperatura | Promedio por estación en tiempo real |
| 2 | Humedad | Humedad relativa promedio |
| 3 | IAQ | Índice de calidad del aire |
| 4 | Presión | Presión atmosférica promedio |
| 5 | Throughput | Eventos procesados por ventana |
| 6 | Latencia | Latencia promedio del pipeline |
| 7 | Eventos por estación | Distribución (pie chart) |

---

## Machine Learning para Predicción de Temperatura

### Notebooks

| Notebook | Modelo | GPU | Enlace |
|---|---|---|---|
| 04 | Prophet | No | [Ver](https://github.com/TU_USUARIO/TU_REPO/blob/main/notebooks/04_modelo_prophet.ipynb) |
| 05 | ARIMA(2,1,1) | No | [Ver](https://github.com/TU_USUARIO/TU_REPO/blob/main/notebooks/05_modelo_arima.ipynb) |
| 06 | XGBoost | Sí (RTX 4050) | [Ver](https://github.com/TU_USUARIO/TU_REPO/blob/main/notebooks/06_modelo_xgboost.ipynb) |
| 07 | LSTM multivariado | Sí (RTX 4050) | [Ver](https://github.com/TU_USUARIO/TU_REPO/blob/main/notebooks/07_modelo_lstm.ipynb) |
| 08 | Comparación + baseline | — | [Ver](https://github.com/TU_USUARIO/TU_REPO/blob/main/notebooks/08_comparacion_modelos.ipynb) |

### Resultados

| Modelo | RMSE | MAE | MAPE (%) | vs Baseline |
|---|---|---|---|---|
| **ARIMA (2,1,1)** | **0.0861** | **0.0720** | **0.62** | **−1% (gana)** |
| Baseline (lag-1) | 0.0870 | 0.0537 | 0.45 | — |
| Prophet | 0.1351 | 0.1162 | 0.96 | +55% |
| LSTM | 0.2056 | 0.1753 | 1.45 | +136% |
| XGBoost | 0.3170 | 0.2511 | 2.07 | +264% |

> **Conclusión:** Con datos limitados (~55 registros post-agregación a 5 min), ARIMA es el modelo más efectivo. Modelos complejos (XGBoost, LSTM) requieren ≥200 muestras para ser competitivos.

---

## Métricas de Rendimiento

| Prueba | Trigger | Watermark | Throughput | Latencia normal | Latencia delay |
|---|---|---|---|---|---|
| 1 | 10s | 30s | 0.5 ev/s | ~0ms | ~5000ms |
| 2 | 10s | 60s | 0.5 ev/s | ~0ms | ~5000ms |
| 3 | 5s | 30s | 0.5 ev/s | ~0ms | ~5000ms |

---

## Repositorio

El código completo del proyecto está disponible en GitHub:

```
https://github.com/TU_USUARIO/TU_REPO
```

### Estructura

```
pipeline-iot-bigdata/
├── docker-compose.yml          # 6 contenedores
├── producer/
│   ├── Dockerfile
│   ├── producer.py             # Productor Kafka
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
└── docs/
    ├── _config.yml
    └── index.md
```

---

## Cómo ejecutar

```bash
# 1. Clonar el repositorio
git clone https://github.com/TU_USUARIO/TU_REPO.git
cd TU_REPO

# 2. Configurar credenciales Supabase (crear .env)
SUPABASE_URL=https://tu-proyecto.supabase.co
SUPABASE_KEY=tu_api_key

# 3. Iniciar infraestructura
docker compose up -d

# 4. Acceder
# Grafana:      http://localhost:3000 (admin/admin)
# Jupyter Lab:  http://localhost:8888
```

---

*Proyecto académico — Curso Big Data, Unidad 2*
