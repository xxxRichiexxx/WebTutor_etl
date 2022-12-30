SELECT id, code, name, modification_date
FROM {0}
WHERE modification_date > CAST('{1}' AS DATETIME2);