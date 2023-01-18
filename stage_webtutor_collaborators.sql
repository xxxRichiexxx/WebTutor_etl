SELECT
    cs.id,
    code,
    fullname,
    email,
    phone,
    mobile_phone,
    birth_date,
    sex,
    COALESCE(position_id, 0)                                                AS position_id,
    position_name,
    COALESCE(position_parent_id, 0)                                         AS position_parent_id,
    position_parent_name,
    COALESCE(org_id, 0)                                                     AS org_id,
    org_name,
    COALESCE(place_id, 0)                                                   AS place_id,
    COALESCE(region_id, 0)                                                  AS region_id,
    role_id,
    is_candidate, 
    COALESCE(candidate_status_type_id, 0)                                   AS candidate_status_type_id,
    is_outstaff,
    is_dismiss,
    position_date,
    hire_date,
    dismiss_date,
    current_state,
    modification_date,
    data.value('(/collaborator/access/web_banned)[1]', 'bit')               AS web_banned
FROM {0} AS cs
JOIN collaborator AS c
    ON cs.id = c.id
WHERE cs.modification_date > CAST('{1}' AS DATETIME2);     