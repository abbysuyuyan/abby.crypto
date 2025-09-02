WITH rng AS (
  SELECT
    current_date - INTERVAL '30' DAY AS start_day,
    current_date                    AS end_day
),
base AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    CAST(amount_usd AS DOUBLE)                  AS vol_usd,
    trader_id
  FROM dex_solana.trades
  WHERE block_time >= (SELECT start_day FROM rng)
    AND block_time <  (SELECT end_day   FROM rng)
),
daily AS (
  SELECT
    day,
    SUM(vol_usd)                      AS day_volume_usd,
    COUNT(DISTINCT trader_id)         AS dau
  FROM base
  GROUP BY 1
)
SELECT
  ROUND(AVG(dau), 0)                         AS avg_dau_30d,          
  ROUND(SUM(day_volume_usd), 2)              AS total_volume_30d_usd, 
  max_by(day, day_volume_usd)                AS max_volume_day,       
  ROUND(MAX(day_volume_usd), 2)              AS max_day_volume_usd   
FROM daily;
