WITH
params AS (
  SELECT
    date_trunc('day', now()) - INTERVAL '1' YEAR AS start_ts,
    date_trunc('day', now()) AS end_ts
),

daily_users AS (
  SELECT
    date_trunc('day', block_time) AS day,
    trader_id AS user,
    MIN(block_time) AS first_trade_time
  FROM dex_solana.trades
  WHERE block_time >= (SELECT start_ts FROM params)
    AND block_time < (SELECT end_ts FROM params)
  GROUP BY 1, 2  
),

daily_stats AS (
  SELECT
    CAST(day AS DATE) AS day,
    COUNT(DISTINCT user) AS dau,
    COUNT(DISTINCT CASE 
      WHEN day = date_trunc('day', first_trade_time) 
      THEN user 
    END) AS new_users
  FROM daily_users
  GROUP BY 1
)

SELECT
  day,
  dau,
  new_users,
  SUM(new_users) OVER (ORDER BY day) AS cumulative_users
FROM daily_stats
ORDER BY day;
