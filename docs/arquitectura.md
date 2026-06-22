# Arquitectura del Pipeline

## Diagrama de flujo

```mermaid
graph LR
    %% ---------------- CAPA 1: CAPTURA ----------------
    subgraph capa_captura [Físico / Captura]
        subgraph sensor_group [Sensores Reales]
            esp32[ESP32 + BME680]
        end
        style sensor_group fill:#eef,stroke:#333,stroke-width:4px;
    end

    %% ---------------- CAPA 2: ALMACENAMIENTO ----------------
    subgraph capa_supabase [Almacenamiento Histórico]
        supabase[Supabase <br>Rest API via httpx]
        style capa_supabase fill:#e6f2ff,stroke:#333;
    end

    %% ---------------- CAPA 3: INGESTA ----------------
    subgraph capa_producer [Simulación / Ingesta]
        python_producer["Python Producer <br>(Lee Supabase + Simula 5 estaciones)"]
        style capa_producer fill:#fdf,stroke:#333;
    end

    %% ---------------- CAPA 4: BROKER ----------------
    subgraph capa_kafka [Broker de Mensajería]
        kafka["Apache Kafka <br>Topic: iot.air_quality.streaming (3 particiones) <br>DLQ: iot.air_quality.streaming.dlq"]
        style capa_kafka fill:#ffe,stroke:#333;
    end

    %% ---------------- CAPA 5: STREAMING ----------------
    subgraph capa_spark [Procesamiento Stream]
        spark_streaming[Spark Streaming <br>Ventana 1min / Slide 30s / Watermark 30s <br>Trigger 10s / ForeachBatch / OutputMode Update]
        style capa_spark fill:#e5e,stroke:#333;
    end

    %% ---------------- CAPA 6: SINK DE TIEMPO ----------------
    subgraph capa_timescale [Base de Datos Temporal]
        timescaledb[TimescaleDB <br>Hypertable para Series de Tiempo]
        style capa_timescale fill:#ede,stroke:#333;
    end

    %% ---------------- CAPA 7: VISUALIZACIÓN ----------------
    subgraph capa_grafana [Observabilidad / BI]
        grafana[Grafana <br>7 Paneles de Monitoreo <br>Refresh 10s]
        style capa_grafana fill:#efe,stroke:#333;
    end

    %% ---------------- CAPA 8: EXPLORACIÓN ----------------
    subgraph capa_exploracion [Exploración Inicial]
        nb01[Notebook 01: Exploración Supabase]
        style capa_exploracion fill:#fcf,stroke:#333;
    end

    %% ---------------- CAPA 9: ML ----------------
    subgraph capa_ml [Machine Learning]
        jupyter[Jupyter Lab <br>GPU RTX 4050 CUDA 12.9]
        subgraph notebooks ["Modelos ML (4 Notebooks)"]
            m1[Random Forest]
            m2[MLP]
            m3[XGBoost]
            m4[LSTM]
        end
        subgraph comparacion [Comparación]
            m5[Notebook 08: Comparativa]
        end
        style capa_ml fill:#fcf,stroke:#333;
        jupyter --> m1 & m2 & m3 & m4
        m1 & m2 & m3 & m4 --> m5
    end

    %% ---------------- CONEXIONES PRINCIPALES ----------------
    %% Datos Físicos -> Almacenamiento
    esp32 -- "datos reales (1 min)" --> supabase
    
    %% Almacenamiento -> Ingesta -> Broker
    supabase -- "lee históricos" --> python_producer
    python_producer -- "simula 5 estaciones" --> kafka

    %% Broker -> Spark -> Sink -> BI
    kafka -- "suscripción streaming" --> spark_streaming
    spark_streaming -- "sink de tiempo" --> timescaledb
    timescaledb -- "7 paneles" --> grafana

    %% ---------------- CONEXIONES ML ----------------
    %% Notebook 01 explora Supabase directamente
    nb01 -- "consulta histórica" --> supabase
    %% Notebooks ML (04-08) leen datos agregados desde TimescaleDB
    jupyter -.-> timescaledb
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
