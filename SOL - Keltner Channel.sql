-- Represent EMA with SMA
WITH d AS (
  SELECT
    CAST(date_trunc('day', minute) AS date) AS day,
    MIN(price) AS low,
    MAX(price) AS high,
    min_by(price, minute) AS open,
    max_by(price, minute) AS close
  FROM prices.usd
  WHERE symbol='SOL'
    AND minute >= current_date - interval '120' day
  GROUP BY 1
),
tr AS (
  SELECT
    day, open, high, low, close,
    GREATEST(
      high - low,
      ABS(high - LAG(close) OVER (ORDER BY day)),
      ABS(low  - LAG(close) OVER (ORDER BY day))
    ) AS true_range
  FROM d
),
k AS (
  SELECT
    day, close,
    AVG( (high+low+close)/3.0 ) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma20_tp,
    AVG(true_range)             OVER (ORDER BY day ROWS BETWEEN 9  PRECEDING AND CURRENT ROW) AS atr10
  FROM tr
)
SELECT
  day,
  close,
  sma20_tp                                                    AS mid,
  sma20_tp + 2*atr10                                          AS kc_upper,
  sma20_tp - 2*atr10                                          AS kc_lower
FROM k
WHERE day >= current_date - interval '90' day
ORDER BY day;
