WITH d AS (
  SELECT
    CAST(date_trunc('day', minute) AS date) AS day,
    MIN(price) AS low,
    MAX(price) AS high,
    max_by(price, minute) AS close
  FROM prices.usd
  WHERE symbol='SOL'
    AND minute >= current_date - interval '120' day
  GROUP BY 1
),
dc AS (
  SELECT
    day, close,
    MAX(high) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS donchian_high_20,
    MIN(low)  OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS donchian_low_20
  FROM d
)
SELECT
  day,
  close,
  donchian_high_20,
  donchian_low_20,
  CASE WHEN close > donchian_high_20 THEN 1 ELSE 0 END AS breakout_up,
  CASE WHEN close < donchian_low_20  THEN 1 ELSE 0 END AS breakout_down
FROM dc
WHERE day >= current_date - interval '90' day
ORDER BY day;
