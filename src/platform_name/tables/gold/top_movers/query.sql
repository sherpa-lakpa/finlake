-- Ranks securities within each trading day by the magnitude of their daily
-- return, largest movers first. `returns` is the temp view name declared
-- by this table's metadata.yaml `paths.input.parameter`.
SELECT
    trade_date,
    security_id,
    daily_return,
    RANK() OVER (
        PARTITION BY trade_date
        ORDER BY ABS(daily_return) DESC
    ) AS movement_rank
FROM returns
WHERE daily_return IS NOT NULL
