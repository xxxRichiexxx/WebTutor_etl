SELECT 
    id, code, name, modification_date, region_id, timezone_id,
    COALESCE(region_id, 0)                                      AS region_id,
    COALESCE(timezone_id, 0)                                    AS timezone_id
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);