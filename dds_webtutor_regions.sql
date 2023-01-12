TRUNCATE TABLE sttgaz.dds_webtutor_regions;
INSERT INTO sttgaz.dds_webtutor_regions
    (
    "region_id",
	code,
	name,
	modification_date
    )
SELECT
	"id",
	code,
	name,
	modification_date
FROM sttgaz.stage_webtutor_regions;