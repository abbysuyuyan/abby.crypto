WITH d AS (
  SELECT CAST(date_trunc('day', minute) AS date) AS day,
         MIN(price) AS low, MAX(price) AS high, max_by(price, minute) AS close
  FROM prices.usd
  WHERE symbol='SOL' AND minute >= current_date - interval '120' day
  GROUP BY 1
),
win AS (
  SELECT
    day, close,
    AVG(close)    OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma20,
    STDDEV(close) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std20,
    AVG(high-low) OVER (ORDER BY day ROWS BETWEEN 9  PRECEDING AND CURRENT ROW) AS atr10_like
  FROM d
),
w AS (
  SELECT
    day, close, sma20, std20,
    4*std20                 AS bb_width,           
    4*atr10_like            AS kc_width          
  FROM win
)
SELECT
  day,
  CASE WHEN bb_width < kc_width THEN 1 ELSE 0 END AS squeeze_flag
FROM w
WHERE day >= current_date - interval '90' day
ORDER BY day;
