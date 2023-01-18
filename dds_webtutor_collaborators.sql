TRUNCATE TABLE sttgaz.dds_webtutor_collaborators;
INSERT INTO sttgaz.dds_webtutor_collaborators
    (
	collaborator_id,
	code,
	fullname,
	email,
	phone,
	mobile_phone,
	birth_date,
	sex,
	position_id,
	position_parent_id,
	org_id,
	place_id,
	region_id,
	role_id,
	is_candidate,
	candidate_status_type_id,
	is_outstaff,
	is_dismiss,
	position_date,
	hire_date,
	dismiss_date,
	current_state,
	modification_date
    )
SELECT
	c.id,
	c.code,
	c.fullname,
	c.email,
	c.phone,
	c.mobile_phone,
	c.birth_date,
	c.sex,
	ps.id,
	c.position_parent_id,
	o.id AS org_id,
	p.id AS place_id,
	r.id AS region_id,
	c.role_id,
	c.is_candidate,
	c.candidate_status_type_id,
	c.is_outstaff,
	CASE
		WHEN c.is_dismiss IS False AND c.web_banned IS True THEN True
		ELSE c.is_dismiss
	END AS is_dismiss,
	c.position_date,
	CASE
		WHEN c.hire_date IS NULL AND c.dismiss_date IS NOT NULL THEN c.dismiss_date
		ELSE c.hire_date
	END AS hire_date,
	CASE
		WHEN c.dismiss_date IS NULL AND c.web_banned IS True THEN c.hire_date
		ELSE c.dismiss_date
	END AS dismiss_date,
	c.current_state,
	c.modification_date
FROM sttgaz.stage_webtutor_collaborators AS c
LEFT JOIN sttgaz.dds_webtutor_orgs AS o
    ON c.org_id = o.org_id 
LEFT JOIN sttgaz.dds_webtutor_places AS p 
    ON c.place_id = p.place_id 
LEFT JOIN sttgaz.dds_webtutor_regions AS r 
    ON c.region_id = r.region_id
LEFT JOIN sttgaz.dds_webtutor_positions AS ps
    ON c.position_id = ps.position_id;