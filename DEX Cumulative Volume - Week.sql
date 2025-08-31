WITH span AS (
  SELECT '7d'  AS period, NOW() - INTERVAL '7' DAY AS ts_from
  UNION ALL
  SELECT '24h' AS period, NOW() - INTERVAL '1' DAY
),
raw_volume AS (
  SELECT
    s.period,
    t.project,
    SUM(CAST(t.amount_usd AS DOUBLE)) AS usd_volume
  FROM dex.trades t
  JOIN span s
    ON t.block_time >= s.ts_from
   AND t.block_time <  NOW()
  GROUP BY 1,2
),
ranked AS (
  SELECT
    period,
    project,
    usd_volume,
    ROW_NUMBER() OVER (PARTITION BY period ORDER BY usd_volume DESC) AS rnk
  FROM raw_volume
)
SELECT
  project,

  CAST(ROUND(SUM(CASE WHEN period = '7d'  THEN usd_volume END)) AS BIGINT)  AS "7d Volume",
  CAST(ROUND(SUM(CASE WHEN period = '24h' THEN usd_volume END)) AS BIGINT)  AS "24h Volume",

  MIN(CASE WHEN period = '7d'  THEN rnk END)  AS "7d Rank",
  MIN(CASE WHEN period = '24h' THEN rnk END)  AS "24h Rank"

FROM ranked
GROUP BY project
ORDER BY "7d Volume" DESC NULLS LAST;
