WITH
params AS (
  SELECT
    date_trunc('day', now()) - INTERVAL '1' YEAR AS start_ts,
    date_trunc('day', now()) AS end_ts
),
daily_users_compressed AS (
  SELECT
    date_trunc('day', block_time) AS day,
    trader_id AS user,
    COUNT(*) as trade_count
  FROM dex_solana.trades
  WHERE block_time >= (SELECT start_ts FROM params)
    AND block_time < (SELECT end_ts FROM params)
  GROUP BY 1, 2  

),
-- 找出每個用戶的首日
user_first_day AS (
  SELECT 
    user,
    MIN(day) AS first_day
  FROM daily_users_compressed
  GROUP BY user
),
-- 計算每日統計
daily_stats AS (
  SELECT
    d.day,
    COUNT(DISTINCT d.user) AS dau,
    COUNT(DISTINCT CASE 
      WHEN d.day = f.first_day THEN d.user 
    END) AS new_users,
    SUM(d.trade_count) AS total_trades
  FROM daily_users_compressed d
  LEFT JOIN user_first_day f ON d.user = f.user
  GROUP BY d.day
)
SELECT
  CAST(day AS DATE) AS day,
  dau,
  new_users,
  total_trades,
  SUM(new_users) OVER (ORDER BY day) AS cumulative_users
FROM daily_stats
ORDER BY day;
