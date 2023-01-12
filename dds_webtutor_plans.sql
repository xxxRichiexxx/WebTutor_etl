TRUNCATE TABLE sttgaz.dds_webtutor_plans;
INSERT INTO sttgaz.dds_webtutor_plans
    (
    subdivision_id,
    plan_name,
    plan_value
    )    
SELECT
    dds.id,
    stage.plan_name,
    stage.plan_value
FROM sttgaz.stage_webtutor_subdivision AS stage
JOIN sttgaz.dds_webtutor_subdivision AS dds
    ON dds.subdivision_id = stage.id;