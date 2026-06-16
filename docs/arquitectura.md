# Arquitectura del Pipeline

## Diagrama de flujo

```mermaid
flowchart LR
    Sensor["ESP32 + BME680"] -->|datos cada 1 min| Supabase[("Supabase\nAlmacenamiento")]
    Supabase -->|httpx REST cada 10s| Producer["Python Producer\n5 estaciones simuladas"]
    Producer -->|estación normal| Kafka{{"Apache Kafka\niot.air_quality.streaming\n3 particiones"}}
    Producer -->|delay 5s - ESP32_05| Kafka
    Kafka -->|eventos fallidos| DLQ[("Dead Letter Queue\ndlq 1 partición")]
    Kafka -->|Spark Structured Streaming\nlatest offset| Spark["Spark Streaming 3.5.0\nVentana 1min / Slide 30s\nWatermark 30s / Trigger 10s\nOutputMode Update"]
    Spark -->|foreachBatch JDBC| TSDB[("TimescaleDB PG15\nHypertable: air_quality_metrics\nÍndices por estación + tiempo")]
    TSDB -->|SQL| ML["Machine Learning\nProphet · ARIMA(2,1,1)\nXGBoost · LSTM\nGPU RTX 4050 (CUDA 12.9)"]
    TSDB -->|SQL| Grafana[("Grafana 10.4.0\n7 paneles\nRefresh 10s")]
    ML -->|predicciones| Grafana
    ML -->|métricas CSV| Compare[("Comparativa\nNotebook 08")]
```

## Componentes Docker

```mermaid
graph TB
    subgraph "Docker Compose - iot-network"
        ZK[Zookeeper\ncp-zookeeper:7.5.0\n2181] --> K[Kafka\ncp-kafka:7.5.0\n9092]
        K --> P[Producer\nPython 3.11\nkafka-python-ng]
        K --> J[Jupyter\npyspark-notebook\nspark-3.5.0\n8888 - GPU]
        J --> TS[TimescaleDB\ntimescaledb:pg15\n5432]
        TS --> G[Grafana\ngrafana:10.4.0\n3000]
    end
```

## Parámetros de Streaming

| Parámetro | Valor |
|---|---|
| Trigger | `processingTime = "10 seconds"` |
| Watermark | `"30 seconds"` |
| Ventana | `1 minute, slide 30 seconds` |
| Output mode | `update` |
| Checkpoint | `/home/jovyan/checkpoint/iot` |
| Spark master | `local[*]` |
