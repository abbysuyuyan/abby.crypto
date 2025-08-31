WITH stable_syms AS (
  SELECT ARRAY['usdc','usdt','dai','tusd','usde','frax','lusd','usds','usdk','susd'] AS syms
),
base AS (
  SELECT
    project,
    DATE_TRUNC('day', block_time) AS day,
    CAST(amount_usd AS DOUBLE)    AS amt_usd,
    lower(COALESCE(token_sold_symbol,  '')) AS sold_sym,
    lower(COALESCE(token_bought_symbol,'')) AS bought_sym
  FROM dex.trades
  WHERE block_time >= DATE_TRUNC('day', NOW()) - INTERVAL '30' day
    AND block_time  < DATE_TRUNC('day', NOW())
)
SELECT
  b.project,
  b.day,
  SUM(
    CASE WHEN (contains(s.syms, b.sold_sym) AND contains(s.syms, b.bought_sym))
           OR (NOT contains(s.syms, b.sold_sym) AND NOT contains(s.syms, b.bought_sym))
         THEN b.amt_usd ELSE 0 END
  ) AS swap_volume_usd
FROM base b
CROSS JOIN stable_syms s
GROUP BY 1,2
ORDER BY b.day, b.project;
