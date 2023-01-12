TRUNCATE TABLE sttgaz.dds_webtutor_places;
INSERT INTO sttgaz.dds_webtutor_places
    (
	"place_id",
	code,
	name,
	modification_date,
	region_id,
	timezone_id
    )
SELECT
	p.id,
	p.code,
	p.name,
	p.modification_date,
	r.id AS region_id,
	p.timezone_id
FROM sttgaz.stage_webtutor_places AS p
LEFT JOIN sttgaz.dds_webtutor_regions AS r
	ON p.region_id = r.region_id;