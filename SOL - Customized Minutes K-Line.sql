SELECT
  from_unixtime(
    floor(to_unixtime(minute) / (60 * {{k_minutes}})) * 60 * {{k_minutes}}
  ) AS bucket,
  min_by(price, minute) AS open,
  MAX(price) AS high,
  MIN(price) AS low,
  max_by(price, minute) AS close
FROM prices.usd
WHERE symbol = 'SOL'
  AND minute >= now() - INTERVAL '1' hour * {{lookback_hours}}
  AND minute < now()
GROUP BY floor(to_unixtime(minute) / (60 * {{k_minutes}}))
ORDER BY bucket;
