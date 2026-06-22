# Pipeline IoT BigData

Pipeline end-to-end para monitoreo ambiental en tiempo real con **Arquitectura Kappa**: Supabase → Kafka → Spark Streaming → TimescaleDB → ML → Grafana.

## Inicio Rápido

```bash
# Clonar y ejecutar
docker compose up -d

# Accesos
JupyterLab:  http://localhost:8888
Grafana:     http://localhost:3000  (admin/admin)
TimescaleDB: localhost:5432  (postgres/postgres)
```

## Stack Tecnológico

| Componente | Tecnología | Propósito |
|---|---|---|
| Fuente de datos | Bosch BME680 + ESP32 + Supabase | Captura de métricas ambientales |
| Ingesta | Apache Kafka 7.5.0 | Buffer distribuido tolerante a fallos |
| Procesamiento | Spark Structured Streaming 3.5.0 | Agregaciones en ventanas con watermark |
| Base de datos | TimescaleDB PG15 | Almacenamiento optimizado para series temporales |
| Visualización | Grafana 10.4.0 | 7 paneles de monitoreo en tiempo real |
| ML | Random Forest, MLP, XGBoost, LSTM | Predicción de temperatura t+10min con 20 features (temp, hum, iaq, pres, eco2 + lags) |
| Orquestación | Docker Compose | 6 contenedores en red aislada |

## Notebooks

| # | Notebook | Descripción |
|---|----------|-------------|
| 01 | Exploración Supabase | Análisis exploratorio de datos históricos |
| 02 | Verificación Kafka | Consumo y validación de eventos Kafka |
| 03 | Pipeline Streaming | Pipeline Spark → TimescaleDB completo |
| 04 | Random Forest | Predicción t+10min con 20 features (bagging) |
| 05 | MLP | Predicción t+10min con red feed-forward (GPU) |
| 06 | XGBoost | Predicción t+10min con gradient boosting (GPU) |
| 07 | LSTM | Predicción t+10min con LSTM multivariado (GPU) |
| 08 | Comparación | Comparativa de los 4 modelos ML + baseline t+10min |

## Resultados ML

| Modelo | RMSE | MAPE (%) | vs Baseline |
|---|---|---|---|
| **XGBoost** 🏆 | **0.2055** | **1.36%** | **−77%** |
| MLP | 0.3445 | 2.61% | −61% |
| Baseline (lag-1) | 0.8829 | 2.38% | — |
| Random Forest | 1.0681 | 3.87% | +21% |
| LSTM | 1.2668 | 6.52% | +43% |

> **XGBoost** ganador con 20 features (temp, humedad, IAQ, presión, eCO2 + lags). Modelos con GPU (XGBoost, MLP) superan al baseline; Random Forest y LSTM overfittean con ~50 muestras.
