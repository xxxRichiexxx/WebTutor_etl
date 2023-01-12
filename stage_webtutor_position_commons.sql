SELECT
    id, code, name,
    COALESCE(position_familys, 0)                 AS position_familys,
    modification_date
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);