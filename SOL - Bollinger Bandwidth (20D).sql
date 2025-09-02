WITH daily AS (
  SELECT
    date_trunc('day', minute)                 AS day,
    max_by(price, minute)                     AS close
  FROM prices.usd
  WHERE symbol = 'SOL'
    AND minute >= current_date - interval '90' day
  GROUP BY 1
),
bb AS (
  SELECT
    day,
    close,
    AVG(close)    OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma20,
    STDDEV(close) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std20
  FROM daily
)
SELECT
  day AS "Date (UTC)",
  CASE WHEN sma20 > 0
       THEN (4*std20) / sma20               
       ELSE NULL
  END AS "Bollinger Bandwidth (20D)"
FROM bb
ORDER BY 1;
