CREATE TABLE IF NOT EXISTS air_quality_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    estacion TEXT NOT NULL,
    avg_temperatura FLOAT,
    avg_humedad FLOAT,
    avg_iaq FLOAT,
    avg_presion FLOAT,
    avg_eco2 FLOAT,
    avg_voc FLOAT,
    evento_count INT,
    latencia_ms FLOAT
);

SELECT create_hypertable('air_quality_metrics', 'window_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_estacion ON air_quality_metrics (estacion);
CREATE INDEX IF NOT EXISTS idx_window_start ON air_quality_metrics (window_start DESC);
