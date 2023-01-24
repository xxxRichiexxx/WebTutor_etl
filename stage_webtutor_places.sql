SELECT 
    id, code, name, modification_date,
    COALESCE(region_id, 0)                                      AS region_id,
    COALESCE(timezone_id, 0)                                    AS timezone_id,
    COALESCE(parent_id, 0)                                      AS parent_object_id
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);