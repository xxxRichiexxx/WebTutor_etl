SELECT
    id, code, name,
    COALESCE(org_id, 0)                             AS org_id,
    COALESCE(parent_object_id, 0)                   AS parent_object_id,
    COALESCE(position_common_id, 0)                 AS position_common_id,
    modification_date
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);