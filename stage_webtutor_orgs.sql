SELECT
    id, code, name, disp_name, modification_date,
    COALESCE(place_id, 0)                           AS place_id,
    COALESCE(region_id, 0)                          AS region_id
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);