SELECT 
    id, code, name,
    COALESCE(org_id, 0)             AS org_id,
    COALESCE(parent_object_id, 0)   AS parent_object_id,
    is_disbanded,
    COALESCE(place_id, 0)           AS place_id,
    COALESCE(cost_center_id, 0)     AS cost_center_id,
    modification_date, 
    COALESCE(region_id, 0)          AS region_id,
    is_faculty
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);