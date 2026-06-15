# Pipeline IoT Big Data - Arquitectura Kappa

Sistema de monitoreo ambiental IoT en tiempo real implementando **Arquitectura Kappa** con Apache Kafka, Apache Spark Structured Streaming, TimescaleDB, Grafana y 4 modelos de Machine Learning (Prophet, ARIMA, XGBoost, LSTM) para predicción de temperatura.

## Descripcion

Procesa datos ambientales (temperatura, humedad, presion, calidad del aire) capturados por un sensor **Bosch BME680** conectado a un **ESP32**. Los datos fluyen en tiempo real a traves de un pipeline de streaming distribuido, se agregan en ventanas temporales y se visualizan en dashboards operacionales. Adicionalmente, se entrenan 4 modelos ML para predecir la temperatura con datos agregados a ventanas de 5 minutos.

## Arquitectura

```
[ESP32 + BME680]  →  datos reales del sensor (1 minuto)
        ↓
   [Supabase]     →  almacenamiento historico (REST API via httpx)
        ↓
[Python Producer] →  lee Supabase + simula 5 estaciones
        ↓
 [Apache Kafka]   →  topic: iot.air_quality.streaming (3 particiones)
        ↓              + DLQ: iot.air_quality.streaming.dlq
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

### Notebooks ML (04-08)

Los datos se agregan a **ventanas de 5 minutos** (~55 registros) y se evaluan con split temporal 80/20:

```
[TimescaleDB] → agregacion 5min → split 80/20 → modelos ML → comparacion
                                                      ↓
                                              GPU (XGBoost, LSTM)
                                              CPU (Prophet, ARIMA)
```

### Principios Kappa aplicados

| Principio | Implementacion |
|---|---|
| Todo es stream | Kafka es la unica fuente del pipeline |
| Sin capa batch separada | Supabase solo es backup, no interviene en el flujo |
| Reprocesamiento posible | Kafka retiene mensajes (retention configurable) |
| Un solo motor de procesamiento | Spark Structured Streaming |
| Tolerancia a fallos | Checkpoint en Spark + replicacion Kafka |

## Tecnologias

| Componente | Tecnologia | Version |
|---|---|---|
| Sensor IoT | Bosch BME680 + ESP32 | - |
| Base de datos | Supabase (REST API via httpx) | - |
| Streaming | Apache Kafka | 7.5.0 |
| Procesamiento | Apache Spark Structured Streaming | 3.5.0 |
| Sink temporal | TimescaleDB (PostgreSQL) | PG15 |
| Visualizacion | Grafana | 10.4.0 |
| Analisis | Jupyter Lab + PySpark | Spark 3.5.0 / Python 3.11 |
| ML Prophet | Facebook Prophet | `prophet` |
| ML ARIMA | Statsmodels (2,1,1) | `statsmodels` |
| ML XGBoost | XGBoost con GPU (`device='cuda'`) | `xgboost` 3.2.0 |
| ML LSTM | TensorFlow con GPU (seed=42) | `tensorflow` 2.21.0 |
| GPU | NVIDIA RTX 4050 (CUDA 13.3, cuDNN 9) | WDDM |
| Lenguaje | Python | 3.11 |
| Infraestructura | Docker + Docker Compose | - |

## Estructura del proyecto

```
pipeline-iot-bigdata/
├── docker-compose.yml              # 6 contenedores + GPU para Jupyter
├── .env                            # Credenciales (no commitear)
├── README.md                       # Este archivo
├── .gitignore
├── producer/
│   ├── Dockerfile                  # Python 3.11
│   ├── producer.py                 # Lee Supabase (httpx) + simula 5 estaciones
│   └── requirements.txt            # kafka-python-ng, httpx, python-dotenv
├── notebooks/
│   ├── 01_exploracion_supabase.ipynb      # Datos historicos Supabase
│   ├── 02_verificacion_kafka.ipynb        # Verificacion de eventos Kafka
│   ├── 03_streaming_pipeline.ipynb        # Pipeline Spark → TimescaleDB
│   ├── 04_modelo_prophet.ipynb            # Prediccion con Prophet
│   ├── 05_modelo_arima.ipynb              # Prediccion con ARIMA(2,1,1)
│   ├── 06_modelo_xgboost.ipynb            # Prediccion con XGBoost (GPU)
│   ├── 07_modelo_lstm.ipynb               # Prediccion con LSTM (GPU, seed=42)
│   └── 08_comparacion_modelos.ipynb       # Comparacion de 4 modelos + baseline
├── grafana/
│   └── dashboards/
│       └── iot_dashboard.json      # Dashboard preconfigurado (7 paneles)
├── timescaledb/
│   └── init.sql                    # Tabla air_quality_metrics + hypertable + indices
├── kafka/
│   └── create_topics.sh            # Creacion de topicos
├── checkpoint/                     # Estado de Spark (persistente, volumen Docker)
└── docs/                           # GitHub Pages
    ├── index.md                    # Landing page del proyecto
    └── .nojekyll                   # Desactiva Jekyll en GitHub Pages
```

## Requisitos

- Docker Desktop instalado y corriendo (con soporte GPU en Windows)
- Python 3.11+ (para desarrollo local)
- 4GB+ de RAM disponibles
- Credenciales de Supabase (URL + clave anon)
- NVIDIA RTX 4050+ (opcional, para GPU en notebooks ML)
- NVIDIA Container Toolkit (para GPU en Docker)

## Configuracion

### 1. Variables de entorno

```bash
cp .env.example .env
```

Edita `.env` con tus credenciales:

```
SUPABASE_URL=https://tu-proyecto.supabase.co
SUPABASE_KEY=eyJ...   # Clave anon (formato JWT)
PRODUCER_INTERVAL=10   # Segundos entre cada envio de datos
```

### 2. Levantar el sistema

```bash
docker-compose up -d --build
```

Esto levanta 6 contenedores:
- `zookeeper` (puerto 2181)
- `kafka` (puerto 9092)
- `timescaledb` (puerto 5432)
- `grafana` (puerto 3000)
- `jupyter` (puerto 8888, con GPU si disponible)
- `producer` (sin puerto expuesto)

### 3. Verificar contenedores

```bash
docker-compose ps
```

## GPU en Jupyter

El contenedor Jupyter esta configurado para usar GPU NVIDIA via `deploy.resources.reservations.devices` en docker-compose.yml. Las librerias CUDA (cudart, cublas, cudnn, nccl, etc.) se instalan via pip como paquetes `nvidia-*`.

```yaml
environment:
  - LD_LIBRARY_PATH=/opt/conda/lib/python3.11/site-packages/nvidia/cuda_runtime/lib:...  # 8 paths nvidia
deploy:
  resources:
    reservations:
      devices:
        - driver: nvidia
          count: all
          capabilities: [gpu]
```

**Nota:** Si el contenedor no detecta GPU, recrealo:
```bash
docker-compose rm -f -s jupyter
docker-compose up -d jupyter
```

## Ejecucion de Notebooks

### Obtener token de Jupyter

```bash
docker logs jupyter 2>&1 | findstr "token="
```

### Acceder a Jupyter

Abre `http://localhost:8888` e ingresa el token.

### Orden de ejecucion

#### Pipeline streaming (orden obligatorio)

| # | Notebook | Funcion |
|---|---|---|
| 1 | `01_exploracion_supabase.ipynb` | Verifica datos historicos del sensor en Supabase |
| 2 | `02_verificacion_kafka.ipynb` | Confirma que el producer esta publicando eventos en Kafka |
| 3 | `03_streaming_pipeline.ipynb` | Ejecuta el pipeline completo: Kafka → Spark → TimescaleDB |

**Nota:** El notebook 03 debe tener la celda de streaming activa para que Grafana muestre datos en vivo.

#### Modelos ML (orden recomendado, independientes del streaming)

| # | Notebook | Modelo | Metrica |
|---|---|---|---|
| 4 | `04_modelo_prophet.ipynb` | Prophet | RMSE=0.1351, MAPE=0.96% |
| 5 | `05_modelo_arima.ipynb` | ARIMA(2,1,1) | **RMSE=0.0861, MAPE=0.62% (mejor)** |
| 6 | `06_modelo_xgboost.ipynb` | XGBoost GPU | RMSE=0.3170, MAPE=2.07% |
| 7 | `07_modelo_lstm.ipynb` | LSTM GPU | RMSE=0.2056, MAPE=1.45% |
| 8 | `08_comparacion_modelos.ipynb` | Comparacion + baseline | ARIMA gana al baseline |

Los notebooks ML leen datos directamente de TimescaleDB y no requieren el streaming activo.

## Resultados ML

| Modelo | RMSE | MAE | MAPE (%) | vs Baseline |
|--------|:----:|:---:|:--------:|:-----------:|
| **ARIMA (2,1,1)** | **0.0861** | **0.0720** | **0.62** | **−1% (gana)** |
| Baseline (lag-1) | 0.0870 | 0.0537 | 0.45 | — |
| Prophet | 0.1351 | 0.1162 | 0.96 | +55% |
| LSTM (seed=42) | 0.2056 | 0.1753 | 1.45 | +136% |
| XGBoost | 0.3170 | 0.2511 | 2.07 | +264% |

**Conclusion:** Con datos limitados (~55 registros post-agregacion 5min), modelos simples (ARIMA, lag-1) superan a redes complejas. ARIMA es el unico modelo que mejora al baseline.

## Configuracion de Grafana

1. Accede a `http://localhost:3000` (admin/admin)
2. **Connections** → **Data Sources** → **Add PostgreSQL**
3. Host: `timescaledb:5432`, Database: `iot_metrics`, User/Pass: `postgres`
4. Importa `grafana/dashboards/iot_dashboard.json`
5. Configura auto-refresh en 10s (icono reloj arriba a la derecha)

### Panel | Descripcion
---|---|---
1 | Temperatura promedio por estacion
2 | Humedad relativa promedio
3 | IAQ con umbrales de color
4 | Presion atmosferica
5 | Throughput (eventos/ventana)
6 | Latencia del pipeline (ms)
7 | Distribucion de eventos (pie chart)

## Parametros del Pipeline

| Parametro | Valor | Justificacion |
|---|---|---|
| Trigger | `processingTime = 10 seconds` | Micro-batch razonable para IoT |
| Ventana | `1 minute`, slide `30 seconds` | Agregaciones con solapamiento |
| Watermark | `30 seconds` | Tolera pequenos retrasos de red |
| Output mode | `update` | Resultados inmediatos sin esperar cierre de ventana |
| Particiones Kafka | 3 | Procesamiento paralelo en Spark |
| Intervalo producer | 10s (configurable via PRODUCER_INTERVAL) | Balance entre carga y frescura |

## Simulacion de sensores

| Estacion | Variacion | Proposito |
|---|---|---|
| ESP32_01 | Datos reales sin modificar | Linea base |
| ESP32_02 | Temperatura ±0.5°C, humedad ±2% | Variacion normal |
| ESP32_03 | Temperatura ±1.2°C, humedad ±3% | Mayor variacion |
| ESP32_04 | IAQ +20 | Zona con mayor polucion |
| ESP32_05 | Delay de 5 segundos | Prueba de watermarking |

## GitHub Pages

El proyecto incluye una carpeta `docs/` para desplegar en GitHub Pages:

1. Subir el repositorio a GitHub
2. Settings → Pages → Source: `main` → `/docs`
3. La pagina queda en `https://TU_USUARIO.github.io/TU_REPO/`

## Troubleshooting

### GPU no disponible en notebooks
Verificar:
```bash
docker exec jupyter nvidia-smi          # GPU visible?
docker exec jupyter python -c "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"
```
Si no funciona, recrear contenedor: `docker-compose rm -f -s jupyter && docker-compose up -d jupyter`

### Producer falla con "Invalid API key"
Usa la clave `anon` (formato `eyJ...`), no `sb_publishable`.

### Spark no escribe en TimescaleDB
Verifica que `timescaledb` este corriendo: `docker ps | grep timescaledb`

### Limpiar checkpoint de Spark
```bash
docker-compose down
rm -rf checkpoint/*
docker-compose up -d
```

### Dashboard no muestra datos
1. Notebook 03 debe tener la celda de streaming activa
2. Auto-refresh en 10s
3. Si "Datasource not found", seleccionar PostgreSQL manualmente en cada panel

### Latencia negativa (~ -900ms)
Normal por diferencia de reloj entre contenedores Docker. No afecta el funcionamiento.

## Autores

Proyecto academico de Big Data - Arquitectura Kappa aplicada a IoT con Machine Learning.
