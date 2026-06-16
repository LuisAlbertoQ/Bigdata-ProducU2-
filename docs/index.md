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
| ML | Prophet, ARIMA, XGBoost, LSTM | Predicción de temperatura con GPU RTX 4050 |
| Orquestación | Docker Compose | 6 contenedores en red aislada |

## Notebooks

| # | Notebook | Descripción |
|---|----------|-------------|
| 01 | Exploración Supabase | Análisis exploratorio de datos históricos |
| 02 | Verificación Kafka | Consumo y validación de eventos Kafka |
| 03 | Pipeline Streaming | Pipeline Spark → TimescaleDB completo |
| 04 | Prophet | Predicción de temperatura con Prophet |
| 05 | ARIMA | Predicción con ARIMA(2,1,1) |
| 06 | XGBoost | Predicción con XGBoost (GPU) |
| 07 | LSTM | Predicción con LSTM multivariado (GPU) |
| 08 | Comparación | Comparativa de los 4 modelos + baseline |

## Resultados ML

| Modelo | RMSE | MAPE (%) | vs Baseline |
|---|---|---|---|
| **ARIMA (2,1,1)** | **0.0861** | **0.62** | **−1%** |
| Baseline (lag-1) | 0.0870 | 0.45 | — |
| Prophet | 0.1351 | 0.96 | +55% |
| LSTM | 0.2056 | 1.45 | +136% |
| XGBoost | 0.3170 | 2.07 | +264% |

> ARIMA(2,1,1) es el mejor modelo. Con ~55 registros (agregación a 5 min), modelos complejos overfittean.
