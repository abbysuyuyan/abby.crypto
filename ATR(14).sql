WITH daily AS (
  SELECT
    CAST(date_trunc('day', minute) AS date) AS day,
    MIN(price)                               AS low,
    MAX(price)                               AS high,
    min_by(price, minute)                    AS open,   
    max_by(price, minute)                    AS close   
  FROM prices.usd
  WHERE symbol = 'SOL'
    AND minute >= current_date - interval '110' day   
  GROUP BY 1
),
  
tr AS (
  SELECT
    day, open, high, low, close,
    GREATEST(
      high - low,
      ABS(high - LAG(close) OVER (ORDER BY day)),
      ABS(low  - LAG(close)  OVER (ORDER BY day))
    ) AS true_range
  FROM daily
),
  
atr AS (
  SELECT
    day,
    close,
    AVG(true_range) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr14
  FROM tr
)
  
SELECT
  day,
  close,
  atr14
FROM atr
WHERE day >= current_date - interval '90' day
ORDER BY day;
