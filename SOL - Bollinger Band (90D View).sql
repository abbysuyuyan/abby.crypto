WITH daily_prices AS (
  SELECT
    date_trunc('day', minute) AS day,
    first_value(price) OVER (PARTITION BY date_trunc('day', minute) ORDER BY minute) AS open,
    last_value(price) OVER (PARTITION BY date_trunc('day', minute) ORDER BY minute 
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close
  FROM prices.usd
  WHERE symbol = 'SOL'
    AND minute >= current_date - interval '90' day
),
unique_daily AS (
  SELECT DISTINCT day, open, close
  FROM daily_prices
),
bollinger_bands AS (
  SELECT
    day,
    open,
    close,
    AVG(close) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
    AVG(close) OVER (ORDER BY day ROWS BETWEEN 39 PRECEDING AND CURRENT ROW) AS ma40,
    AVG(close) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma30,
    STDDEV(close) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS std30
  FROM unique_daily
)
SELECT
  day,
  open,
  close,
  ma20,
  ma40,
  sma30,
  sma30 + 2 * std30 AS upper_band,
  sma30 - 2 * std30 AS lower_band
FROM bollinger_bands
WHERE day >= current_date - interval '90' day
ORDER BY day;
