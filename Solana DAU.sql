WITH
params AS (
  SELECT
    date_trunc('day', now()) - INTERVAL '1' YEAR AS start_ts,
    date_trunc('day', now()) AS end_ts
),

unique_daily_users AS (
  SELECT DISTINCT
    date_trunc('day', block_time) AS day,
    trader_id AS user
  FROM dex_solana.trades
  WHERE block_time >= (SELECT start_ts FROM params)
    AND block_time < (SELECT end_ts FROM params)
),

user_first_day AS (
  SELECT 
    user,
    MIN(day) AS first_day
  FROM unique_daily_users
  GROUP BY user
),

daily_stats AS (
  SELECT
    u.day,
    COUNT(DISTINCT u.user) AS dau,
    COUNT(DISTINCT f.user) AS new_users
  FROM unique_daily_users u
  LEFT JOIN user_first_day f ON u.day = f.first_day AND u.user = f.user
  GROUP BY u.day
)
SELECT
  CAST(day AS DATE) AS day,
  dau,
  new_users,
  SUM(new_users) OVER (ORDER BY day) AS cumulative_users
FROM daily_stats
ORDER BY day;
