WITH
sq AS (
	SELECT
	--	*
		COALESCE(subs.id, sub.id) AS id,
		CASE
			WHEN subs.code = sub.code THEN subs.code
			WHEN subs.code <> sub.code THEN CONCAT(CONCAT(subs.code, '&'), sub.code)
			ELSE COALESCE(subs.code, sub.code)
		END AS sub_code,
		CASE
			WHEN subs.name = sub.name THEN subs.name
			WHEN subs.name <> sub.name THEN CONCAT(CONCAT(subs.name, '&'), sub.name)
			ELSE COALESCE(subs.name, sub.name)
		END AS sub_name,
		CASE
			WHEN subs.org_id = sub.org_id THEN subs.org_id::varchar
			WHEN subs.org_id <> sub.org_id THEN CONCAT(CONCAT(subs.org_id::varchar, '&'), sub.org_id::varchar)
			ELSE COALESCE(subs.org_id, sub.org_id)::varchar
		END AS org_id,
		CASE
			WHEN subs.place_id = sub.place_id THEN subs.place_id::varchar
			WHEN subs.place_id <> sub.place_id THEN CONCAT(CONCAT(subs.place_id::varchar, '&'), sub.place_id::varchar)
			ELSE COALESCE(subs.place_id, sub.place_id)::varchar
		END AS place_id,
		CASE
			WHEN subs.is_disbanded = sub.is_disbanded THEN subs.is_disbanded::varchar 
			WHEN subs.is_disbanded <> sub.is_disbanded THEN CONCAT(CONCAT(subs.is_disbanded, '&'), sub.is_disbanded)
			ELSE COALESCE(subs.is_disbanded, sub.is_disbanded)::varchar
		END AS is_disbanded,
		CASE
			WHEN subs.region_id = sub.region_id THEN subs.region_id::varchar 
			WHEN subs.region_id <> sub.region_id THEN CONCAT(CONCAT(subs.region_id, '&'), sub.region_id)
			ELSE COALESCE(subs.region_id, sub.region_id)::varchar
		END AS region_id,
		created,
		modified,
		xml_id,
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
		plan_name,
		plan_value	
	FROM sttgaz.stage_webtutor_subdivisions AS subs
	RIGHT JOIN sttgaz.stage_webtutor_subdivision AS sub
		USING(id)
)
SELECT
	sq.*,
	o.id AS org_org_id,
	o.code AS org_code,
	o.name AS org_name,
	o.disp_name AS org_disp_name,
	o.region_id AS org_region_id,
	r.id AS region_region_id,
	r.code AS region_code,
	r.name AS region_name,
	p.id AS place_place_id
FROM sq
--WHERE created <> xml_creation_date
--WHERE modified <> xml_modification_date
--WHERE code <> f_nliz
LEFT JOIN sttgaz.stage_webtutor_orgs AS o
	ON sq.org_id::int = o.id
--WHERE o.id IS NULL
--WHERE INSTR(sq.f_so4s, o.name) < 1
LEFT JOIN sttgaz.stage_webtutor_regions AS r
	ON sq.region_id::int = r.id
--WHERE region_region_id IS NULL
LEFT JOIN sttgaz.stage_webtutor_places AS p
	ON sq.place_id::int = p.id
	
