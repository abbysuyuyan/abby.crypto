WITH
params AS (
  SELECT
    CAST(date_trunc('day', now()) - INTERVAL '1' YEAR AS DATE) AS start_day,
    CAST(date_trunc('day', now()) - INTERVAL '1' DAY AS DATE) AS end_day
),
-- 一次掃描，同時標記新用戶
user_activity AS (
  SELECT
    CAST(date_trunc('day', block_time) AS DATE) AS day,
    trader_id AS user,
    ROW_NUMBER() OVER (PARTITION BY trader_id ORDER BY block_time) AS rn
  FROM dex_solana.trades
  WHERE block_time >= (SELECT start_day FROM params)
    AND block_time < (SELECT end_day FROM params) + INTERVAL '1' DAY
),
-- 聚合每日數據
daily_stats AS (
  SELECT
    day,
    COUNT(DISTINCT user) AS dau,
    COUNT(DISTINCT CASE WHEN rn = 1 THEN user END) AS new_users
  FROM user_activity
  GROUP BY day
),
-- 生成完整日期序列
calendar AS (
  SELECT CAST(d AS DATE) AS day
  FROM UNNEST(SEQUENCE(
    (SELECT start_day FROM params),
    (SELECT end_day FROM params),
    INTERVAL '1' DAY
  )) AS t(d)
)
-- 最終結果
SELECT
  c.day,
  COALESCE(ds.dau, 0) AS dau,
  COALESCE(ds.new_users, 0) AS new_users,
  SUM(COALESCE(ds.new_users, 0)) OVER (
    ORDER BY c.day 
    ROWS UNBOUNDED PRECEDING
  ) AS cumulative_users
FROM calendar c
LEFT JOIN daily_stats ds ON c.day = ds.day
ORDER BY c.day;
