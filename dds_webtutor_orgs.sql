TRUNCATE TABLE sttgaz.dds_webtutor_orgs;
INSERT INTO sttgaz.dds_webtutor_orgs
    (
	org_id,
	code,
	name,
	disp_name,
	modification_date,
	place_id,
	region_id    
    )
SELECT
	o.id,
	o.code,
	o.name,
	o.disp_name,
	o.modification_date,
	p.id AS place_id,
	r.id AS region_id
FROM sttgaz.stage_webtutor_orgs AS o
LEFT JOIN sttgaz.dds_webtutor_places AS p
	ON p.place_id = o.place_id
LEFT JOIN sttgaz.dds_webtutor_regions AS r
	ON r.region_id = o.region_id;