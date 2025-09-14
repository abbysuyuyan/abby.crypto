SELECT
  from_unixtime(
    floor(to_unixtime(minute) / (3600 * {{k_hours}})) * 3600 * {{k_hours}}
  ) AS bucket,
  min_by(price, minute) AS open,
  MAX(price) AS high,
  MIN(price) AS low,
  max_by(price, minute) AS close
FROM prices.usd
WHERE symbol = 'SOL'
  AND minute >= now() - INTERVAL '1' HOUR * {{lookback_hours}}
  AND minute < now()
GROUP BY floor(to_unixtime(minute) / (3600 * {{k_hours}}))
ORDER BY bucket;
