# Pipeline IoT Big Data - Arquitectura Kappa

Sistema de monitoreo ambiental IoT en tiempo real implementando **Arquitectura Kappa** con Apache Kafka, Apache Spark Structured Streaming, TimescaleDB y Grafana.

## Descripcion

Este proyecto procesa datos ambientales (temperatura, humedad, presion, calidad del aire) capturados por un sensor **Bosch BME680** conectado a un **ESP32**. Los datos fluyen en tiempo real a traves de un pipeline de streaming distribuido, se agregan en ventanas temporales y se visualizan en dashboards operacionales.

## Arquitectura

```
[ESP32 + BME680]  →  datos reales del sensor
        ↓
   [Supabase]     →  almacenamiento historico
        ↓
[Python Producer] →  lee Supabase + simula 5 estaciones
        ↓
 [Apache Kafka]   →  topico: iot.air_quality.streaming
        ↓
 [Spark Streaming] →  ventanas (1min), watermark (30s), agregaciones
        ↓ (foreachBatch)
 [TimescaleDB]    →  sink de series de tiempo
        ↓
   [Grafana]      →  dashboards BI + observabilidad
        ↑
  [Jupyter Lab]   →  notebooks interactivos de analisis
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
| Base de datos | Supabase | - |
| Streaming | Apache Kafka | 7.5.0 |
| Procesamiento | Apache Spark Structured Streaming | 3.5.0 |
| Sink | TimescaleDB (PostgreSQL) | PG15 |
| Visualizacion | Grafana | 10.4.0 |
| Analisis | Jupyter Lab + PySpark | Spark 3.5.0 / Python 3.11 |
| Lenguaje | Python | 3.11 |
| Infraestructura | Docker + Docker Compose | - |

## Estructura del proyecto

```
pipeline-iot-bigdata/
├── docker-compose.yml              # 6 contenedores
├── .env                            # Credenciales (no commitear)
├── .env.example                    # Plantilla de variables
├── README.md                       # Este archivo
├── producer/
│   ├── Dockerfile                  # Imagen Python 3.11
│   ├── producer.py                 # Lee Supabase + simula 5 sensores
│   └── requirements.txt            # kafka-python, supabase, dotenv
├── notebooks/
│   ├── 01_exploracion_supabase.ipynb   # Datos historicos
│   ├── 02_verificacion_kafka.ipynb     # Verificacion de eventos
│   └── 03_streaming_pipeline.ipynb     # Pipeline completo con Spark
├── grafana/
│   └── dashboards/
│       └── iot_dashboard.json      # Dashboard preconfigurado
├── timescaledb/
│   └── init.sql                    # Tabla + hypertable + indices
├── kafka/
│   └── create_topics.sh            # Creacion de topicos
└── checkpoint/                     # Estado de Spark (persistente)
```

## Requisitos

- Docker Desktop instalado y corriendo
- Python 3.11+ (para desarrollo local)
- 4GB+ de RAM disponibles
- Credenciales de Supabase (URL + clave anon)

## Configuracion

### 1. Variables de entorno

```bash
cp .env.example .env
```

Edita `.env` con tus credenciales:

```
SUPABASE_URL=https://tu-proyecto.supabase.co
SUPABASE_KEY=eyJ...   # Clave anon (no sb_publishable)
PRODUCER_INTERVAL=10   # Segundos entre cada envio de datos
```

**Importante:** La clave debe ser la `anon public` (formato JWT `eyJ...`), **no** la `sb_publishable`. Se obtiene en Supabase → Settings → API.

### 2. Levantar el sistema

```bash
docker-compose up -d --build
```

**Importante:** Usa `--build` en el primer arranque o después de modificar el `producer.py` para reconstruir la imagen. Si solo reinicias, usa:

```bash
docker-compose restart producer
```

Esto levanta 6 contenedores:
- `zookeeper` (puerto 2181)
- `kafka` (puerto 9092)
- `timescaledb` (puerto 5432)
- `grafana` (puerto 3000)
- `jupyter` (puerto 8888)
- `producer` (sin puerto expuesto)

### 3. Verificar contenedores

```bash
docker-compose ps
```

Todos deben estar en estado `Up`.

## Ejecucion de Notebooks

### Obtener token de Jupyter

```bash
docker logs jupyter 2>&1 | findstr "token="
```

### Acceder a Jupyter

Abre `http://localhost:8888` e ingresa el token.

### Orden de ejecucion

| # | Notebook | Funcion |
|---|---|---|
| 1 | `01_exploracion_supabase.ipynb` | Verifica datos historicos del sensor en Supabase |
| 2 | `02_verificacion_kafka.ipynb` | Confirma que el producer esta publicando eventos en Kafka |
| 3 | `03_streaming_pipeline.ipynb` | Ejecuta el pipeline completo: Kafka → Spark → TimescaleDB |

Cada notebook se ejecuta celda por celda (Shift+Enter). El notebook 03 inicia queries de streaming que quedan activas hasta que se detengan explicitamente.

## Configuracion de Grafana

1. Accede a `http://localhost:3000`
2. Login: `admin` / `admin`
3. Ve a **Connections** → **Data Sources** → **Add PostgreSQL**
4. Configuracion:
   - Host: `timescaledb:5432`
   - Database: `iot_metrics`
   - User: `postgres`
   - Password: `postgres`
5. Guarda y prueba la conexion
6. Importa el dashboard desde `grafana/dashboards/iot_dashboard.json`

## Parametros del Pipeline

| Parametro | Valor | Justificacion |
|---|---|---|
| Trigger | `processingTime = 10 seconds` | Micro-batch razonable para IoT |
| Ventana | `1 minute`, slide `30 seconds` | Agregaciones con solapamiento |
| Watermark | `30 seconds` | Tolera pequenos retrasos de red |
| Output mode | `update` | Resultados inmediatos sin esperar cierre de ventana |
| Checkpoint | `/home/jovyan/checkpoint/iot` | Persistente entre reinicios |

## Simulacion de sensores

El producer genera 5 estaciones virtuales basadas en datos reales:

| Estacion | Variacion | Proposito |
|---|---|---|
| ESP32_01 | Datos reales sin modificar | Linea base |
| ESP32_02 | Temperatura ±0.5°C, humedad ±2% | Variacion normal |
| ESP32_03 | Temperatura ±1.2°C, humedad ±3% | Mayor variacion |
| ESP32_04 | IAQ +20 | Zona con mayor polucion |
| ESP32_05 | Delay de 5 segundos | Prueba de watermarking |

## Troubleshooting

### Producer falla con "Invalid API key"
Usa la clave `anon` (formato `eyJ...`), no `sb_publishable`.

### Spark no escribe en TimescaleDB
Verifica que el contenedor `timescaledb` este corriendo: `docker ps | grep timescaledb`

### Jupyter no encuentra paquetes
Ejecuta la celda de instalacion de dependencias al inicio de cada notebook.

### Reiniciar un contenedor especifico
```bash
docker-compose restart producer
```

### Limpiar checkpoint de Spark
Si hay errores de estado corrupto:
```bash
docker-compose down
rm -rf checkpoint/*
docker-compose up -d
```

### Latencia negativa en Grafana
Valores de ~ -900ms son normales. Se debe a diferencias de reloj entre contenedores Docker. No afecta el funcionamiento del pipeline.

### Dashboard no muestra datos
1. Verifica que el notebook 03 este ejecutandose con la celda 7 activa.
2. Configura el auto-refresh del dashboard en 10s (icono del reloj arriba a la derecha).
3. Si al importar el JSON aparece "Datasource not found", selecciona manualmente la conexion PostgreSQL en cada panel.

## Autores

Proyecto academico de Big Data - Arquitectura Kappa aplicada a IoT.
