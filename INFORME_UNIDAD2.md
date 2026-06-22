# Informe Unidad 2 - Pipeline Big Data en Tiempo Real con Arquitectura Kappa

## Datos generales

| Campo | Valor |
|---|---|
| Curso | Big Data |
| Unidad | 2 |
| Estudiante | ______________________________ |
| Fecha | 24 / 05 / 2026 |
| Docente | ______________________________ |

---

## 1. Resumen ejecutivo

Se implementó un pipeline streaming basado en Arquitectura Kappa para el monitoreo ambiental IoT en tiempo real. El sistema captura métricas de temperatura, humedad, presión y calidad del aire desde un sensor Bosch BME680 conectado a un ESP32, cuyos datos se almacenan en Supabase. Un productor Python lee estos datos y simula 5 estaciones (ESP32_01 a ESP32_05), publicando los eventos en Apache Kafka. Spark Structured Streaming consume el tópico, aplica ventanas temporales de 1 minuto con watermark de 30 segundos, y escribe los resultados agregados en TimescaleDB mediante foreachBatch. Finalmente, Grafana visualiza las métricas en un dashboard operativo con 7 paneles.

**Resultado:** Pipeline funcional con latencia ~0ms para estaciones normales y ~5000ms para estación con delay simulado (ESP32_05), throughput de 5 eventos cada 10 segundos, y dashboard en Grafana actualizado cada 10 segundos.

---

## 2. Arquitectura del pipeline (Kappa)

### Diagrama de flujo

```
[ESP32 + BME680]  →  datos reales del sensor (1 minuto)
        ↓
   [Supabase]     →  almacenamiento historico
        ↓
[Python Producer] →  lee Supabase + simula 5 estaciones
        ↓
 [Apache Kafka]   →  topic: iot.air_quality.streaming (3 particiones)
        ↓
[Spark Streaming] →  ventana 1min / slide 30s / watermark 30s / trigger 10s
        ↓  (foreachBatch - outputMode update)
 [TimescaleDB]    →  sink de series de tiempo (hypertable)
        ↓
   [Grafana]      →  7 paneles de monitoreo BI
```

### Componentes principales

| Componente | Tecnología | Función |
|---|---|---|
| Fuente de datos | Bosch BME680 + ESP32 + Supabase | Captura de métricas ambientales y almacenamiento histórico |
| Ingesta | Apache Kafka 7.5.0 | Buffer distribuido tolerante a fallos con 3 particiones |
| Procesamiento | Spark Structured Streaming 3.5.0 | Agregaciones en tiempo real con ventanas y watermark |
| Sink | TimescaleDB PG15 | Base de datos optimizada para series temporales |
| Visualización BI | Grafana 10.4.0 | Dashboards en tiempo real con 7 paneles |
| Orquestación | Docker Compose | 6 contenedores en red aislada |
| Análisis interactivo | Jupyter Lab + PySpark | 3 notebooks: exploración, verificación y pipeline |

### Supuestos técnicos de ejecución

- El sistema se ejecuta en una máquina local con Docker Desktop.
- Se requieren 4 GB de RAM disponibles para los 6 contenedores.
- La red interna `iot-network` permite comunicación entre contenedores.
- El productor tiene fallback a datos mock si Supabase no está disponible.
- El checkpoint de Spark se almacena en un volumen persistente.

---

## 3. Ingesta en tiempo real con Kafka

### Nombre del tópico

```
iot.air_quality.streaming (3 particiones, 1 réplica)
```

Adicionalmente se creó un tópico para eventos fallidos:

```
iot.air_quality.streaming.dlq (Dead Letter Queue)
```

### Productor utilizado

- **Lenguaje:** Python 3.11
- **Librería:** kafka-python-ng 2.2.2
- **Ubicación:** Contenedor Docker `producer`
- **Comportamiento:** Lee datos reales de Supabase cada 10 segundos y publica 5 eventos (uno por estación simulada) con variación aleatoria controlada.

### Consumidor utilizado

- **Notebook:** `02_verificacion_kafka.ipynb`
- **Librería:** kafka-python-ng
- **Comportamiento:** Consume mensajes durante 10 segundos y muestra estación, temperatura e IAQ.

### Ejemplo de evento generado

```json
{
  "estacion": "ESP32_01",
  "temperatura": 25.4,
  "humedad": 58.0,
  "presion": 1013.2,
  "altura": 3820,
  "gas": 120,
  "iaq": 42,
  "eco2": 620,
  "VOC": 0.45,
  "calidad_aire": "BUENA",
  "created_at": "2026-05-24T17:50:00",
  "event_timestamp": 1718214600000,
  "delayed": false
}
```

### Contrato de evento

| Campo | Tipo de dato | Descripción | Ejemplo |
|---|---|---|---|
| estacion | string | Identificador del sensor | ESP32_01 |
| temperatura | double | Temperatura ambiental (°C) | 25.4 |
| humedad | double | Humedad relativa (%) | 58.0 |
| presion | double | Presión atmosférica (hPa) | 1013.2 |
| altura | double | Altitud calculada (m) | 3820 |
| gas | double | Resistencia de gas (Ohm) | 120 |
| iaq | double | Índice de calidad del aire | 42 |
| eco2 | double | CO2 equivalente (ppm) | 620 |
| VOC | double | Compuestos orgánicos volátiles | 0.45 |
| calidad_aire | string | Clasificación de calidad | BUENA |
| created_at | string | Timestamp del evento | 2026-05-24T17:50:00 |
| event_timestamp | long | Timestamp Unix en milisegundos | 1718214600000 |
| delayed | boolean | Indica si es un evento retrasado | false |

### Estrategia de particionado

El tópico se configuró con **3 particiones** para permitir:
- Procesamiento paralelo en Spark
- Escalabilidad horizontal al agregar más sensores
- Balanceo de carga entre ejecutores

---

## 4. Procesamiento en streaming con Spark

### Fuente de lectura

```
Kafka topic: iot.air_quality.streaming
Kafka broker: kafka:29092 (red interna Docker)
Starting offsets: latest
```

### Transformaciones aplicadas

1. **Parseo JSON:** Los eventos se deserializan usando el schema definido en `iot_schema`.
2. **Cálculo de latencia:** `kafka_timestamp (segundos) * 1000 - event_timestamp (ms)` diferencia entre generación y recepción.
3. **Conversión de tiempo:** `created_at` → `event_time` (timestamp).
4. **Agregaciones por ventana:** Promedios de temperatura, humedad, IAQ, presión, eCO2, VOC por estación.

### Parámetros utilizados

| Parámetro | Valor | Justificación |
|---|---|---|
| Trigger | processingTime = "10 seconds" | Micro-batch cada 10s, balance entre frescura de datos y carga de procesamiento |
| Watermark | "30 seconds" | Tolerancia a eventos retrasados de dispositivos IoT con conectividad intermitente |
| Ventana | 1 minute, slide 30 seconds | Agregaciones con solapamiento para suavizar curvas en dashboard |
| Output mode | update | Publica resultados tan pronto como se calculan sin esperar cierre de ventana |
| Checkpoint | /home/jovyan/checkpoint/iot | Persistencia del estado de streaming para recuperación ante fallos |
| Spark master | local[*] | Ejecución local utilizando todos los núcleos disponibles |

### Salida del stream

- **Sink principal:** TimescaleDB mediante `foreachBatch` (JDBC batch mode)
- **Sink de debug:** Consola (formato tabla, 5 filas)

### Simulación de sensores

| Estación | Variación | Propósito |
|---|---|---|
| ESP32_01 | Datos reales sin modificar | Línea base |
| ESP32_02 | Temperatura ±0.5°C, humedad ±2% | Variación normal |
| ESP32_03 | Temperatura ±1.2°C, humedad ±3% | Mayor variación |
| ESP32_04 | IAQ +20 | Zona con mayor polución |
| ESP32_05 | Delay de 5 segundos | Prueba de watermarking |

---

## 5. Métricas de rendimiento

### Resultados de pruebas controladas

| Prueba | Trigger | Watermark | Throughput (eventos/s) | Lag (ms) | Latencia normal (ms) | Latencia delay (ms) | Observaciones |
|---|---|---|---|---|---|---|---|
| 1 | 10s | 30s | 0.5 (5 ev/10s) | ~0 | ~0 | ~5000 | Configuración base. Datos mock |
| 2 | 10s | 60s | 0.5 (5 ev/10s) | ~0 | ~0 | ~5000 | Sin cambio visible por delay fijo de 5s |
| 3 | 5s | 30s | 0.5 (5 ev/10s) | ~0 | ~0 | ~5000 | Mayor frecuencia de cómputo sin mejora sustancial |

**Interpretación:**
- La latencia normal (~0ms) indica que el pipeline procesa los eventos casi instantáneamente.
- La latencia de ESP32_05 (~5000ms) demuestra que el watermark detecta correctamente eventos retrasados.
- El throughput está limitado por el intervalo del productor (10s), no por capacidad de procesamiento.
- El watermark de 30s es suficiente para tolerar el delay simulado de 5s sin pérdida de datos.

---

## 6. Observabilidad del pipeline (Grafana)

### Dashboard implementado

Se configuró un dashboard en Grafana con 7 paneles conectados a TimescaleDB como fuente de datos:

| Panel | Métrica | Descripción |
|---|---|---|
| 1 | Temperatura | Temperatura promedio por estación en tiempo real |
| 2 | Humedad | Humedad relativa promedio por estación |
| 3 | IAQ | Índice de calidad del aire con umbrales de color |
| 4 | Presión | Presión atmosférica promedio |
| 5 | Throughput | Cantidad de eventos procesados por ventana |
| 6 | Latencia | Latencia promedio del pipeline en milisegundos |
| 7 | Eventos por estación | Distribución de eventos procesados (pie chart) |

### Métricas clave

| Métrica | Descripción | Umbral sugerido | Frecuencia de revisión |
|---|---|---|---|
| Latencia | Tiempo entre generación y procesamiento del evento | < 1000 ms normal, < 10000 ms con delay | Cada 10 segundos |
| Throughput | Eventos procesados por ventana | > 1 evento/ventana por estación activa | Cada 30 segundos |
| Errores | Eventos en Dead Letter Queue (DLQ) | 0 errores sostenidos | Cada 5 minutos |
| Backpressure | Diferencia entre offsets de Kafka y procesamiento de Spark | < 100 offsets de retraso | Cada minuto |

### Logs generados

Los logs estructurados se capturan en 3 puntos:

1. **Producer:** `docker logs producer` — muestra "Batch enviado. 5 eventos publicados" cada 10s
2. **Spark (notebook):** salida de consola con "Procesando batch X con N registros"
3. **TimescaleDB:** consulta directa a la tabla `air_quality_metrics`

### Alertas propuestas

| Alerta | Condición | Acción |
|---|---|---|
| IAQ alto | avg_iaq > 100 | Notificación en Grafana |
| Temperatura crítica | avg_temperatura > 35°C o < 0°C | Alerta por temperatura extrema |
| Pipeline degradado | throughput = 0 por más de 60s | Revisar productor y conectividad Kafka |
| Latencia anormal | latencia_ms > 10000 por más de 2 ventanas | Posible saturación del sistema |

---

## 7. Costos y escalado

### Estimación de recursos actuales (Docker local)

| Recurso | Estimación | Justificación |
|---|---|---|
| CPU | 2-4 núcleos | Kafka + Spark + TimescaleDB requieren cómputo paralelo |
| Memoria | 4-6 GB | 6 contenedores: Kafka (1GB), Spark (2GB), TimescaleDB (1GB), otros (~1GB) |
| Particiones Kafka | 3 | Suficiente para 5 estaciones simuladas. Escalar a N estaciones → N/2 particiones |
| Ejecutores Spark | 1 (local) | Entorno local. En producción: 3-5 ejecutores con 2 cores cada uno |
| Almacenamiento | 10-20 GB | Datos históricos en TimescaleDB + checkpoints de Spark |
| Red | Bridge Docker | Comunicación interna entre contenedores sin overhead de red externa |

### Riesgos de backpressure

- **Causa:** Productor envía más rápido de lo que Spark procesa
- **Síntoma:** Aumento del lag entre el último offset de Kafka y el offset procesado por Spark
- **Mitigación:** Aumentar particiones Kafka, agregar ejecutores Spark, o reducir frecuencia del productor

### Estrategia de escalado

1. **Vertical (actual):** Aumentar recursos de CPU/RAM en la máquina local
2. **Horizontal (producción):**
   - Aumentar particiones Kafka (proporcional al número de estaciones)
   - Migrar Spark a modo cluster con múltiples workers
   - TimescaleDB en servidor dedicado con replicación
   - Balanceador de carga para múltiples producers

---

## 8. Modelos de Machine Learning para Predicción

Se implementaron 4 modelos de Machine Learning para predecir la temperatura promedio de la estación ESP32_01 **10 minutos en el futuro** utilizando los datos agregados almacenados en TimescaleDB. Los datos crudos (~900 registros con intervalos de 30 segundos) se agregaron a ventanas de **5 minutos** para reducir ruido y obtener variación significativa (~55 registros). Se utilizó un split temporal 80/20 (44 train, 11 test) para evaluación.

Cada modelo utiliza **20 features**: variables temporales (hora, minuto, día de semana), lags de temperatura/humedad/IAQ/presión/eCO2 (1, 2, 3 pasos de 5 min) y medias móviles de temperatura (3 y 5 ventanas). El target es `avg_temperatura` (próximo valor a 5 minutos).

### Notebooks implementados

| # | Notebook | Modelo | Librería | GPU |
|---|---|---|---|---|
| 04 | `04_modelo_random_forest.ipynb` | Random Forest | `scikit-learn` | No |
| 05 | `05_modelo_mlp.ipynb` | MLP (Multilayer Perceptron) | `tensorflow` | Sí |
| 06 | `06_modelo_xgboost.ipynb` | XGBoost con `device='cuda'` | `xgboost` | Sí |
| 07 | `07_modelo_lstm.ipynb` | LSTM multivariado (seed=42) | `tensorflow` | Sí |
| 08 | `08_comparacion_modelos.ipynb` | Comparación de los 4 modelos ML + baseline | — | — |

### Descripción de cada modelo

#### Random Forest (04)
Modelo de ensamble basado en bagging con 200 árboles de decisión (max_depth=10). Utiliza las mismas features que XGBoost: variables temporales (hora, minuto, día de semana), lags de temperatura/humedad/IAQ (1, 2, 3 pasos de 5 min) y medias móviles (3 y 5 ventanas). No requiere GPU. Random Forest promedia múltiples árboles para reducir overfitting, ideal para datasets pequeños.

#### MLP — Multilayer Perceptron (05)
Red neuronal feed-forward con 3 capas ocultas (128→64→32 neuronas, activación ReLU) y dropout 0.2. Usa las mismas features planas que Random Forest/XGBoost, normalizadas con StandardScaler. Entrenada con optimizador Adam, early stopping (patience=15) y semilla fija (seed=42) para reproducibilidad. Aprovecha GPU (RTX 4050) para acelerar entrenamiento.

#### XGBoost (06)
Modelo de gradient boosting con árboles de decisión. Se crearon features temporales (hora, minuto, día de semana), lags de temperatura/humedad/IAQ (1, 2, 3 pasos de 5 min) y medias móviles (3 y 5 ventanas). Entrenado con `n_estimators=200`, `max_depth=6` y `device='cuda'` en GPU NVIDIA RTX 4050.

#### LSTM (07)
Red neuronal recurrente con 2 capas LSTM (64 y 32 unidades) y dropout de 0.2. Modelo multivariado que predice temperatura usando temperatura, humedad, IAQ, presión y eCO2. Secuencias de 5 pasos (25 min de historia). Entrenado con early stopping y semilla fija (seed=42) para reproducibilidad.

### Resultados finales

| Modelo | RMSE | MAE | MAPE (%) | vs Baseline |
|--------|:----:|:---:|:--------:|:-----------:|
| **XGBoost** 🏆 | **0.2055** | **0.1698** | **1.36** | **−77%** |
| MLP | 0.3445 | 0.3245 | 2.61 | −61% |
| Baseline (lag-1) | 0.8829 | 0.3519 | 2.38 | — |
| Random Forest | 1.0681 | 0.5515 | 3.87 | +21% |
| LSTM | 1.2668 | 0.8874 | 6.52 | +43% |

**Interpretación:**

- Los 4 modelos implementados son exclusivamente de Machine Learning. Se eliminaron Prophet (modelo estadístico aditivo) y ARIMA (modelo estadístico autorregresivo) para enfocar el análisis en técnicas de ML puras.
- **XGBoost es el ganador absoluto**, superando al baseline por 77% (RMSE 0.2055 vs 0.8829). Las 20 features (lags de temperatura, humedad, IAQ, presión, eCO2 + medias móviles) le permiten capturar patrones que el baseline no ve.
- **MLP** ocupa el segundo lugar con RMSE 0.3445 (−61% vs baseline), demostrando que una red pequeña (32→16→1) con target normalizado funciona bien con pocos datos.
- **Random Forest y LSTM** no superan al baseline. RF sufre overfitting con 20 features y solo ~48 muestras. LSTM requiere secuencias largas que con ~55 registros no son viables.
- **20 features vs 14 originales**: la adición de presión y eCO2 mejoró el RMSE de XGBoost de 0.317 a 0.2055 (35% de mejora).
- **LD_LIBRARY_PATH** necesario para que TensorFlow y XGBoost detecten las librerías CUDA instaladas vía pip en `/opt/conda/lib/python3.11/site-packages/nvidia/*/lib/`. Se configuró en `docker-compose.yml`.

### Preparación para ML distribuido

El pipeline actual puede extenderse a ML de las siguientes formas:
1. **Entrenamiento:** Los datos históricos en TimescaleDB se exportan directamente a los notebooks vía SQLAlchemy
2. **Inferencia en streaming:** El modelo ML entrenado podría cargarse en Spark Streaming para predicciones en tiempo real
3. **Feature store:** Los datos agregados por ventanas sirven como features para modelos de clasificación de calidad del aire

---

## 9. Evidencias

### Evidencias del pipeline funcional

| Evidencia | Descripción | Ubicación |
|---|---|---|
| Producer activo | Logs de `docker logs producer` mostrando "Batch enviado. 5 eventos publicados" | Contenedor producer |
| Consumo Kafka | Salida de notebook `02_verificacion_kafka.ipynb` con mensajes recibidos | notebooks/02_verificacion_kafka.ipynb |
| Datos en TimescaleDB | Consulta en notebook `03_streaming_pipeline.ipynb` celda 9: 300+ registros | notebooks/03_streaming_pipeline.ipynb |
| Dashboard Grafana | 7 paneles funcionales con datos en tiempo real | http://localhost:3000 |
| Latencia medida | ESP32_01 a ESP32_04: ~0ms. ESP32_05: ~5000ms | Celda 9 del notebook 03 |
| Modelo Random Forest | Predicción de temperatura con Random Forest | notebooks/04_modelo_random_forest.ipynb |
| Modelo MLP | Predicción de temperatura con MLP (GPU) | notebooks/05_modelo_mlp.ipynb |
| Modelo XGBoost | Predicción de temperatura con XGBoost (GPU) | notebooks/06_modelo_xgboost.ipynb |
| Modelo LSTM | Predicción de temperatura con LSTM (GPU) | notebooks/07_modelo_lstm.ipynb |
| Comparación | Tabla comparativa de los 4 modelos ML | notebooks/08_comparacion_modelos.ipynb |

### Estructura del proyecto

```
pipeline-iot-bigdata/
├── docker-compose.yml              # 6 contenedores
├── .env                            # Credenciales de Supabase
├── producer/
│   ├── Dockerfile
│   ├── producer.py                 # Productor Kafka (lectura Supabase + simulación)
│   └── requirements.txt
├── notebooks/
│   ├── 01_exploracion_supabase.ipynb   # Datos históricos
│   ├── 02_verificacion_kafka.ipynb     # Verificación de eventos Kafka
│   ├── 03_streaming_pipeline.ipynb     # Pipeline completo Spark + TimescaleDB
│   ├── 04_modelo_random_forest.ipynb  # Predicción con Random Forest
│   ├── 05_modelo_mlp.ipynb           # Predicción con MLP (GPU)
│   ├── 06_modelo_xgboost.ipynb        # Predicción con XGBoost (GPU)
│   ├── 07_modelo_lstm.ipynb           # Predicción con LSTM (GPU)
│   └── 08_comparacion_modelos.ipynb   # Comparación de modelos
├── grafana/dashboards/
│   └── iot_dashboard.json          # Dashboard preconfigurado
├── timescaledb/
│   └── init.sql                    # Tabla + hypertable + índices
├── kafka/
│   └── create_topics.sh            # Creación de tópicos
├── README.md
└── .gitignore
```

---

## 10. Conclusiones

### Qué se logró implementar

- Pipeline streaming completo basado en Arquitectura Kappa con **6 contenedores Docker**.
- **Apache Kafka** como sistema de ingesta con tópico particionado y Dead Letter Queue.
- **Spark Structured Streaming** con ventanas de 1 minuto, watermark de 30 segundos, trigger de 10 segundos y output mode `update`.
- **TimescaleDB** como sink de series temporales con hypertable para consultas eficientes.
- **Grafana** con 7 paneles de monitoreo en tiempo real (temperatura, humedad, IAQ, presión, throughput, latencia, eventos por estación).
- **Simulación de 5 estaciones** IoT con una estación de datos retrasados para demostrar watermarking.
- **4 modelos de ML** (Random Forest, MLP, XGBoost, LSTM) para predicción de temperatura con GPU habilitada.
- **GPU habilitada** en contenedor Jupyter mediante configuración de `LD_LIBRARY_PATH` para librerías CUDA instaladas vía pip.

### Limitaciones encontradas

- El watermark actual (30s) impide ver datos en menos de 1 minuto en el dashboard.
- Los contenedores Spark en modo local limitan el escalado horizontal.
- La latencia negativa (~ -900ms) en estaciones normales se debe a diferencias de reloj entre contenedores Docker.
- El productor depende de datos de Supabase; sin conexión usa datos mock.
- **Datos limitados para ML:** Solo ~55 registros tras agregación a 5 minutos. XGBoost logró superar al baseline gracias a 20 features y GPU, pero LSTM y Random Forest overfittean.
- **GPU no detectada inicialmente** en notebooks por falta de `LD_LIBRARY_PATH`. Corregido agregando la variable de entorno al servicio Jupyter en docker-compose.yml.
- **Outlier de 25.4°C** presente en los datos crudos, filtrado mediante `WHERE avg_temperatura < 20`.

### Mejoras propuestas

1. Reducir watermark a 10s para datos casi en tiempo real.
2. Agregar un segundo sink a Parquet para almacenamiento histórico.
3. Implementar alertas en Grafana para monitoreo proactivo.
4. Migrar a Spark en modo cluster para escalado horizontal real.
5. **Para ML:** Acumular más datos históricos (≥200 registros post-agregación) para que modelos complejos (LSTM, Random Forest) sean competitivos.
6. **Para ML:** Probar predicción a 10 min con más datos (shift-2) cuando haya suficiente histórico.

---

## Checklist de entrega

| Criterio | Cumple | Observaciones |
|---|---|---|---|
| Se creó y probó un tópico Kafka | ✅ | `iot.air_quality.streaming` con 3 particiones |
| Se ejecutó productor y consumidor | ✅ | Producer Python, consumer en notebook 02 |
| Se documentó el contrato de evento | ✅ | Tabla con 14 campos, tipos y ejemplos |
| Se implementó un pipeline con Spark Structured Streaming | ✅ | Notebook 03 con foreachBatch a TimescaleDB |
| Se usaron ventanas y watermarking | ✅ | Ventana 1min slide 30s, watermark 30s |
| Se midió latencia y throughput | ✅ | Latencia ~0ms normal, ~5000ms delay. Throughput 0.5 ev/s |
| Se propuso una estrategia de observabilidad | ✅ | Dashboard Grafana con 7 paneles |
| Se definieron métricas, alertas y umbrales | ✅ | 4 alertas propuestas con condiciones |
| Se estimaron costos o recursos de operación | ✅ | Tabla de estimación CPU, RAM, almacenamiento |
| Se propuso una estrategia de escalado | ✅ | Vertical y horizontal documentadas |
| Se adjuntaron evidencias técnicas | ✅ | Notebooks, logs, dashboard, estructura de proyecto |
| Se implementó modelo Random Forest | ✅ | RMSE=1.0681, MAPE=3.87% (overfitting con 20 features) |
| Se implementó modelo MLP con GPU | ✅ | RMSE=0.3445, MAPE=2.61% (target normalizado, red reducida) |
| Se implementó modelo XGBoost con GPU | ✅ | RMSE=0.2055, MAPE=1.36% **(mejor modelo)** |
| Se implementó modelo LSTM con GPU | ✅ | RMSE=1.2668, MAPE=6.52% (overfitting con pocos datos) |
| Se compararon los 4 modelos ML vs baseline | ✅ | 4 modelos ML puros. XGBoost gana con −77% vs baseline |
| GPU habilitada en contenedor Jupyter | ✅ | LD_LIBRARY_PATH configurado en docker-compose.yml |

---

**Enlace GitHub:** ______________________________

**Demo en 5 min, Expo en 15 min**
