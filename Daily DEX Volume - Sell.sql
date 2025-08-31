WITH stable_syms AS (
  SELECT ARRAY['usdc','usdt','dai','tusd','usde','frax','lusd','usds','usdk','susd'] AS syms
),
base AS (
  SELECT
    project,
    date_trunc('day', block_time) AS day,
    CAST(amount_usd AS DOUBLE)    AS amt_usd,
    lower(coalesce(token_sold_symbol,  '')) AS sold_sym,
    lower(coalesce(token_bought_symbol,'')) AS bought_sym
  FROM dex.trades
  WHERE block_time >= date_trunc('day', now()) - INTERVAL '30' day
    AND block_time  < date_trunc('day', now())
)
SELECT
  b.project,
  b.day,
  SUM(CASE
        WHEN NOT contains(s.syms, b.sold_sym) AND contains(s.syms, b.bought_sym)
        THEN b.amt_usd ELSE 0
      END) AS sell_volume_usd
FROM base b
CROSS JOIN stable_syms s
GROUP BY 1,2
ORDER BY b.day, b.project;
