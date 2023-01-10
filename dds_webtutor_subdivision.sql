
INSERT INTO sttgaz.dds_webtutor_subdivision
	(
	subdivision_id,
	code,
	"name",
	org_id,
	is_disbanded,
	place_id,
	region_id,
	formed_date,
	is_faculty,
	"comment",
	creator_id,
	xml_creation_date,
	modificator_id,
	xml_modification_date,
	type_dc,
	site_diler,
	tel,
	street,
	id_stoyanki,
	f_20u4,
	id_plowadki,
	email,
	gps,
	skidki,
	doc_obor,
	f_4xwo,
	f_nliz,
	f_so4s,
	f_postpril1,
	f_dilerskidki
	)
SELECT DISTINCT
	s.id AS subdivision_id,
	s.code,
	s.name,
	o.id AS org_id,
	s.is_disbanded,
	p.id AS place_id,
	r.id AS region_id,
	formed_date,
	is_faculty,
	"comment",
	NULLIF(creator_id, 0) AS creator_id,
	xml_creation_date AS xml_creation_date,
	NULLIF(modificator_id, 0) AS modificator_id,
	xml_modification_date AS xml_modification_date,
	type_dc,
	site_diler,
	tel,
	street,
	id_stoyanki,
	f_20u4,
	id_plowadki,
	email,
	gps,
	skidki,
	doc_obor,
	f_4xwo,
	f_nliz,
	f_so4s,
	f_postpril1,
	f_dilerskidki	
FROM sttgaz.stage_webtutor_subdivision AS s
LEFT JOIN sttgaz.dds_webtutor_places AS p
	ON p.place_id = s.place_id
LEFT JOIN sttgaz.dds_webtutor_regions AS r
	ON r.region_id = s.region_id
LEFT JOIN sttgaz.dds_webtutor_orgs AS o
	ON o.org_id = s.org_id;