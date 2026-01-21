INSERT INTO gold.flow_monthly_anomalies (
    year,
    month,
    calamar_anomaly,
    bolivar_anomaly,
    manaos_anomaly,
    obidos_anomaly,
    tabatinga_anomaly,
    timbues_anomaly
)
WITH base AS (
  SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM silver.flow_monthly WHERE calamar_monthly   IS NOT NULL
  UNION ALL
  SELECT year, month, 'bolivar',        bolivar_monthly        FROM silver.flow_monthly WHERE bolivar_monthly        IS NOT NULL
  UNION ALL
  SELECT year, month, 'manaos',         manaos_monthly         FROM silver.flow_monthly WHERE manaos_monthly         IS NOT NULL
  UNION ALL
  SELECT year, month, 'obidos',         obidos_monthly         FROM silver.flow_monthly WHERE obidos_monthly         IS NOT NULL
  UNION ALL
  SELECT year, month, 'tabatinga',      tabatinga_monthly      FROM silver.flow_monthly WHERE tabatinga_monthly      IS NOT NULL
  UNION ALL
  SELECT year, month, 'timbues',        timbues_monthly        FROM silver.flow_monthly WHERE timbues_monthly        IS NOT NULL
),
stats AS (
  SELECT
    station,
    month,
    AVG(value)          AS mean_month,
    STDDEV_SAMP(value)  AS std_month
  FROM base
  GROUP BY station, month
),
anom AS (
  SELECT
    b.year,
    b.month,
    b.station,
    (b.value - s.mean_month) / NULLIF(s.std_month, 0) AS anomaly
  FROM base b
  JOIN stats s
    ON b.station = s.station
   AND b.month   = s.month
)
SELECT
  year,
  month,
  MAX(anomaly) FILTER (WHERE station = 'calamar')   AS calamar_anomaly,
  MAX(anomaly) FILTER (WHERE station = 'bolivar')   AS bolivar_anomaly,
  MAX(anomaly) FILTER (WHERE station = 'manaos')    AS manaos_anomaly,
  MAX(anomaly) FILTER (WHERE station = 'obidos')    AS obidos_anomaly,
  MAX(anomaly) FILTER (WHERE station = 'tabatinga') AS tabatinga_anomaly,
  MAX(anomaly) FILTER (WHERE station = 'timbues')   AS timbues_anomaly
FROM anom
GROUP BY year, month
ORDER BY year, month;