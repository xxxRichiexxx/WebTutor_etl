TRUNCATE TABLE sttgaz.dds_webtutor_positions;
INSERT INTO sttgaz.dds_webtutor_positions
    (
        position_id,
        code,
        "name",
        org_id,
        parent_object_id,
        position_common_id,
        modification_date
    )
SELECT
    p.id,
    p.code,
    p."name",
    o.id,
    p.parent_object_id,
    pc.id,
    p.modification_date    
FROM sttgaz.stage_webtutor_positions AS p
JOIN sttgaz.dds_webtutor_position_commons AS pc
    ON p.position_common_id = pc.position_common_id
JOIN sttgaz.dds_webtutor_orgs AS o 
    ON p.org_id = o.org_id;