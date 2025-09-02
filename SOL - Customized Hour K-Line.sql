WITH p AS (
  SELECT
    CAST({{k_hours}} AS INTEGER)         AS k_h,
    CAST({{lookback_hours}} AS INTEGER)  AS h_back
),
base AS (
  SELECT
    CAST(floor(to_unixtime(minute) / (3600 * (SELECT k_h FROM p))) AS BIGINT) AS bucket_id,
    minute,
    price
  FROM prices.usd
  WHERE symbol = 'SOL'
    AND minute >= now() - INTERVAL '1' HOUR * (SELECT h_back FROM p)
    AND minute <  now()
),
agg AS (
  SELECT
    bucket_id,
    MIN(price)            AS low,
    MAX(price)            AS high,
    min_by(price, minute) AS open,   
    max_by(price, minute) AS close   
  FROM base
  GROUP BY 1
)
SELECT
  from_unixtime(bucket_id * 3600 * (SELECT k_h FROM p)) AS bucket,
  open, high, low, close
FROM agg
ORDER BY bucket;
