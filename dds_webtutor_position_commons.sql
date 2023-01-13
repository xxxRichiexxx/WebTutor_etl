TRUNCATE TABLE sttgaz.dds_webtutor_position_commons;
INSERT INTO sttgaz.dds_webtutor_position_commons
    (
        position_common_id,
        code,
        "name",
        position_familys,
        modification_date
    )
SELECT *
FROM sttgaz.stage_webtutor_position_commons;