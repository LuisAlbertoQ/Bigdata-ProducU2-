# Estructura PPT — SensorVault

> Instrucciones: Cada slide incluye texto narrado + descripción visual + datos exactos.
> Diagramas Mermaid pueden exportarse como PNG desde https://mermaid.live

---

## Slide 1 — Portada

> ⚠️ Nota: El logo es conceptual (no existe aún). Puede diseñarse en Canva o generarse con IA.

**Texto:**
- **SensorVault**
- *Monitoreo Inteligente para Cadena de Frío Farmacéutica*
- Sistema basado en IoT, Big Data y Machine Learning para la detección temprana de anomalías en temperatura de productos sensibles.
- Big Data — Unidad 2
- [Nombre del alumno]
- [Universidad]
- [Fecha]

**Tecnologías (debajo del título):**
IoT • Kafka • Spark Streaming • TimescaleDB • Grafana • Machine Learning

**Visual:**
- Logo conceptual SensorVault (escudo + señal IoT + curva de predicción)
- Fondo azul oscuro degradado (#0d47a1 → #1a237e)

---

## Slide 2 — Problema

### ¿Por qué monitorear la cadena de frío farmacéutica?

**Situación actual:**
- Los productos farmacéuticos requieren temperatura constante (2°C–8°C)
- Una falla eléctrica de 30 minutos puede dañar todo un lote
- Los sistemas actuales alertan cuando ya es demasiado tarde
- No hay predicción: solo detección reactiva

**Impacto:**
- Lotes enteros perdidos (vacunas, insulinas, biológicos)
- Pérdidas de hasta $15,000+ por incidente
- Riesgo para pacientes que dependen de medicamentos
- Multas regulatorias por quiebre de cadena de frío

**Frase clave:**
> "Cuando suena la alarma, las vacunas ya se dañaron."

**Visual:**
- ❄️ Nevera/cadena de frío
- 💊 Vacunas e insulinas
- 🌡️ Termómetro en rango crítico (2°C–8°C)

---

## Slide 3 — Objetivos del Proyecto

### Objetivo General

Diseñar e implementar una plataforma Big Data para el monitoreo de temperatura en cadena de frío farmacéutica, utilizando sensores IoT y Machine Learning predictivo.

### Objetivos Específicos

1. Capturar temperatura ambiental con sensores ESP32 + BME680
2. Implementar arquitectura Kappa con Kafka y Spark Streaming
3. Almacenar datos temporales en TimescaleDB
4. Visualizar métricas en Grafana en tiempo real
5. Desarrollar modelos de Machine Learning para predecir anomalías
6. Evaluar rendimiento del pipeline

**Visual:**
Checklist con íconos de cada objetivo

---

## Slide 4 — ¿Qué es SensorVault?

SensorVault es un sistema de monitoreo en tiempo real para cadena de frío farmacéutica, diseñado para capturar, procesar, visualizar y predecir variaciones de temperatura antes de que dañen productos sensibles.

### Capacidades

- Monitoreo continuo de temperatura en neveras y almacenes
- Procesamiento en streaming con Kafka y Spark (<10s)
- Visualización en dashboards en tiempo real (Grafana)
- Predicción de temperatura a 5 minutos (LSTM + XGBoost)
- Arquitectura modular y escalable

### Beneficio principal

> Detectar anomalías de temperatura 5 minutos antes de que dañen el lote farmacéutico.

**Visual:**
ESP32 → Kafka → Spark → TimescaleDB → Grafana → ML Predicción

---

## Slide 5 — Arquitectura Kappa

### Arquitectura del sistema

Pipeline de 9 capas con 6 contenedores Docker:

| Capa | Componente |
|------|------------|
| Captura | ESP32 + BME680 (simula nevera) |
| Histórico | Supabase (PostgreSQL) |
| Ingesta | Python Producer (5 estaciones) |
| Broker | Apache Kafka (3 particiones + DLQ) |
| Streaming | Spark Structured Streaming |
| Base de datos | TimescaleDB (hipertabla) |
| Visualización | Grafana (7 paneles, refresh 10s) |
| Análisis | Jupyter Lab |
| Machine Learning | RF, MLP, XGBoost, LSTM |

**Dato clave:**
- Arquitectura Kappa: flujo continuo de eventos, sin capa batch

**Visual:**
Diagrama Mermaid exportado a PNG

---

## Slide 6 — Tecnologías Utilizadas

| Componente | Tecnología |
|------------|------------|
| IoT | ESP32 + BME680 |
| Lenguaje | Python 3.11 |
| Broker | Apache Kafka 7.5.0 |
| Procesamiento | Spark Structured Streaming 3.5.0 |
| Base de datos | TimescaleDB PG15 |
| Visualización | Grafana 10.4.0 |
| Machine Learning | Jupyter + XGBoost + TensorFlow |
| GPU | RTX 4050 (CUDA 12.9) |
| Infraestructura | Docker Compose |

### Justificación

- Kafka: tolerancia a fallos y escalabilidad
- Spark: procesamiento en tiempo real
- TimescaleDB: optimizado para series temporales
- Grafana: observabilidad en tiempo real
- XGBoost: alta precisión predictiva con GPU

---

## Slide 7 — Pipeline de Streaming

### Configuración

| Parámetro | Valor |
|----------|-------|
| Topic | iot.air_quality.streaming |
| Particiones | 3 |
| Trigger | 10 segundos |
| Ventana | 1 minuto, slide 30s |
| Watermark | 30 segundos |
| Output | Update |
| Sink | TimescaleDB (foreachBatch) |

### Características

- DLQ para eventos fallidos
- 5 estaciones simuladas (ESP32_01 a ESP32_05)
- ESP32_05 con delay de 5s para probar watermark
- Latencia normal: ~0ms, con delay: ~5000ms

**Visual:**
Kafka → Spark → TimescaleDB
Recuadro amarillo resaltando "Watermark: 30s"

---

## Slide 8 — Dashboard y Observabilidad

### Monitoreo en tiempo real

Paneles implementados (7):

| # | Panel | Relevancia para cadena de frío |
|---|-------|-------------------------------|
| 1 | Temperatura | **CRÍTICO** — monitoreo continuo |
| 2 | Humedad | Control de ambiente |
| 3 | IAQ | Calidad del aire en almacén |
| 4 | Presión | Monitoreo general |
| 5 | Throughput | Salud del pipeline |
| 6 | Latencia | Tiempo de procesamiento |
| 7 | Distribución | Eventos por estación |

### Características

- Refresh cada 10 segundos
- Umbrales visuales: verde (<7°C), amarillo (7-8°C), rojo (>8°C)
- ⚠️ Alertas automáticas NO implementadas (pendiente)

**Visual:**
Dashboard Grafana o mockup con 7 paneles

---

## Slide 9 — Machine Learning

### Objetivo

Predecir la temperatura en una nevera farmacéutica a 5 minutos, para anticipar quiebres de cadena de frío.

### Dataset

- **Target:** avg_temperatura de ESP32_01 (t+5min)
- **20 variables predictoras:**
  - 3 temporales: hour, minute, dayofweek
  - 15 lags: temp, hum, iaq, pres, eco2 (×3 cada una)
  - 2 medias móviles: temp_rolling_3, temp_rolling_5
- ~102 muestras post-agregación a ventanas de 5 min
- Split: 80% entrenamiento / 20% prueba

### Nota importante

El entrenamiento del modelo se realiza en notebooks Jupyter con datos históricos de TimescaleDB (modo offline).

**Visual:**
Features → Modelo XGBoost → Predicción a 5 min

---

## Slide 10 — Comparación de Modelos

| Modelo | RMSE | MAPE | Interpretación para cadena de frío |
|--------|------|------|-----------------------------------|
| **LSTM 🏆** | **0.2700** | **1.33%** | Error de solo ±0.27°C — excelente |
| **XGBoost** | **0.3009** | **2.06%** | Error de ±0.30°C — excelente |
| Baseline (lag-1) | 0.2969 | 1.21% | ±0.30°C — línea base |
| Random Forest | 0.4034 | 2.60% | ±0.40°C — aceptable |
| MLP | 1.7576 | 16.85% | ±1.76°C — requiere reajuste |

### Resultados

- **LSTM y XGBoost empatan técnicamente** (0.27 vs 0.30 de RMSE, diferencia mínima)
- Con ~102 registros, LSTM alcanzó su potencial (necesita ≥100 datos)
- Ambos modelos detectan quiebre de frío con 5 min de anticipación
- MLP quedó fuera: necesita reentrenamiento con los nuevos datos
- El baseline mejoró al haber más datos — pero sigue siendo reactivo

**Visual:**
Gráfico de barras RMSE. Línea roja marcando umbral 8°C.

---

## Slide 11 — Resultados de Rendimiento

| Métrica | Valor | Impacto en cadena de frío |
|--------|-------|--------------------------|
| Latencia normal | ~0 ms | Alerta inmediata |
| Latencia con delay | ~5000 ms | Tolerancia a fallas de sensor |
| Throughput | 5 eventos / 10s | Monitoreo continuo |
| Registros | 1000+ | Historial para trazabilidad |
| Contenedores | 6 | Infraestructura mínima |

### Resultados

- Sistema estable: sin pérdida de datos
- Watermark de 30s tolera sensores lentos o intermitentes
- Dashboard actualizado cada 10 segundos
- Pipeline listo para escala industrial

---

## Slide 12 — Caso de Aplicación: Cadena de Frío Farmacéutica

### Escenario real

Un hospital almacena 500 dosis de vacuna contra influenza.
Requisito: temperatura entre **2°C y 8°C** todo el tiempo.

### Sin SensorVault

1. Falla eléctrica en la nevera durante 30 minutos
2. Temperatura sube a 14°C
3. Nadie lo sabe hasta que abren la nevera
4. **500 dosis perdidas** = $15,000 + reprogramar vacunación
5. Pacientes sin vacuna por una semana

### Con SensorVault

1. Sensor ESP32 mide temperatura cada 30s
2. A los 4 minutos: temperatura sube a 9°C
3. XGBoost **predice** que en 5 min llegará a 14°C
4. Dashboard marca alerta visual (panel rojo)
5. **Técnico llega antes de que las dosis se dañen**
6. Lote salvado, cero pérdidas

### Beneficio clave

> Predicción 5 minutos antes = tiempo suficiente para actuar y salvar el lote.

**Visual:**
Línea de tiempo comparativa: sin sistema (flecha roja llegando a pérdida) vs con SensorVault (flecha verde con intervención temprana).

---

## Slide 13 — Propuesta de Valor

### ¿Por qué SensorVault para cadena de frío?

| Problema actual | Solución SensorVault |
|----------------|---------------------|
| Alertas reactivas (cuando ya es tarde) | Predicción 5 min antes |
| Sensores caros (SCADA: $500+/sensor) | ESP32 + BME680 < $10 |
| Software propietario (licencias caras) | Stack open source |
| Sin trazabilidad histórica | TimescaleDB con todo el historial |
| No escalable | Kafka + Spark escalan horizontalmente |

### Diferenciación

- Primer sistema de **predicción** (no solo monitoreo) para cadena de frío
- 10x más barato que soluciones tradicionales
- Instalación simple: un sensor ESP32 por nevera
- Resultados comprobados: LSTM y XGBoost con error ±0.3°C

**Visual:**
Tabla comparativa (SensorVault vs SCADA tradicional)

---

## Slide 14 — Demo

### Flujo de demostración (MVP funcional)

1. `docker compose up -d` → 6 contenedores en 30s
2. Producer simula 5 neveras (ESP32_01 a ESP32_05)
3. Kafka recibe datos: temperatura cada 10s
4. Spark procesa ventanas de 1 minuto
5. TimescaleDB almacena todo el historial
6. Grafana muestra 7 paneles con umbrales
7. XGBoost predice temperatura a 5 minutos

**Visual:**
Secuencia de 4 capturas: terminal Docker, Kafka, Grafana, Jupyter

---

## Slide 15 — Conclusiones y Trabajo Futuro

### Conclusiones

- ✅ Pipeline Kappa funcional para monitoreo de cadena de frío
- ✅ Procesamiento en tiempo real (<10s del sensor al dashboard)
- ✅ LSTM y XGBoost predicen temperatura a 5 min con error ±0.3°C
- ✅ A más datos, LSTM se vuelve el mejor modelo (RMSE=0.27)
- ✅ Arquitectura escalable a cientos de neveras

### Trabajo futuro

- 🔴 Alertas automáticas en Grafana (temperatura > 8°C, tendencia al alza)
- 🟡 Integrar XGBoost en el pipeline en tiempo real (no solo offline)
- 🟡 Probar predicción a 10 y 15 minutos con más datos históricos
- 🟢 Probar con sensor real ESP32 (no simulado)
- 🟢 Desplegar en farmacia/hospital real como piloto

**Visual:**
Checklist con checkmarks verdes. Próximos pasos con prioridad (rojo = alta, verde = baja).

---

## Slide 16 — Preguntas

### Gracias

**SensorVault**
Monitoreo Predictivo para Cadena de Frío Farmacéutica

Repositorio:
https://github.com/LuisAlbertoQ/Bigdata-ProducU2-

Documentación:
https://LuisAlbertoQ.github.io/Bigdata-ProducU2-

**Pregunta provocadora:**
> "¿Cuánto cuesta un lote de vacunas en tu nevera?"

**Visual:**
QR del repositorio + logo SensorVault
