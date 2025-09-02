WITH
params AS (
  SELECT
    CAST(date_trunc('day', now()) - INTERVAL '1' YEAR AS DATE) AS start_day,
    CAST(date_trunc('day', now()) - INTERVAL '1' DAY  AS DATE) AS end_day
),

base AS (
  SELECT
    CAST(date_trunc('day', block_time) AS DATE) AS day,
    trader_id AS user
  FROM dex_solana.trades
  WHERE block_time >= (SELECT CAST(start_day AS TIMESTAMP) FROM params)
    AND block_time <  (SELECT CAST(end_day   AS TIMESTAMP) FROM params) + INTERVAL '1' DAY
),

first_seen AS (
  SELECT user, MIN(day) AS first_day
  FROM base
  GROUP BY 1
),

daily_active AS (
  SELECT day, COUNT(DISTINCT user) AS dau
  FROM base
  GROUP BY 1
),

daily_new AS (
  SELECT first_day AS day, COUNT(*) AS new_users
  FROM first_seen
  GROUP BY 1
),

dates AS (
  SELECT SEQUENCE(
           (SELECT start_day FROM params),
           (SELECT end_day   FROM params),
           INTERVAL '1' DAY
         ) AS d_arr
),
calendar AS (
  SELECT CAST(d AS DATE) AS day
  FROM dates
  CROSS JOIN UNNEST(d_arr) AS t(d)
),

merged AS (
  SELECT
    c.day,
    COALESCE(da.dau, 0)       AS dau,
    COALESCE(dn.new_users, 0) AS new_users
  FROM calendar c
  LEFT JOIN daily_active da ON c.day = da.day
  LEFT JOIN daily_new   dn ON c.day = dn.day
)

SELECT
  day,
  dau,
  new_users,
  SUM(new_users) OVER (ORDER BY day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_users
FROM merged
ORDER BY day;
