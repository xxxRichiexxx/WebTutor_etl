DROP TABLE IF EXISTS stage_webtutor_collaborators;

CREATE TABLE stage_webtutor_collaborators (
    id INT, 
    code VARCHAR,
    fullname VARCHAR(500),
    login VARCHAR,
    short_login VARCHAR,
    lowercase_login VARCHAR,
    email VARCHAR,
    phone VARCHAR(500),
    mobile_phone VARCHAR,
    birth_date TIMESTAMP,
    sex VARCHAR,
    pict_url VARCHAR,
    position_id INT, 
    position_name VARCHAR(500),
    position_parent_id INT, 
    position_parent_name VARCHAR(500),
    org_id INT, 
    org_name VARCHAR(1000),
    place_id INT, 
    region_id INT, 
    category_id INT, 
    web_banned BOOLEAN,
    is_arm_admin BOOLEAN,
    is_content_admin BOOLEAN,
    is_application_admin BOOLEAN,
    role_id VARCHAR,
    is_candidate BOOLEAN,
    candidate_status_type_id INT, 
    candidate_id INT, 
    is_outstaff BOOLEAN,
    is_dismiss BOOLEAN,
    position_date TIMESTAMP,
    hire_date TIMESTAMP,
    dismiss_date TIMESTAMP,
    in_request_black_list BOOLEAN,
    allow_personal_chat_request BOOLEAN,
    level_id BOOLEAN,
    knowledge_parts VARCHAR,
    tags VARCHAR,
    experts VARCHAR,
    person_object_profile_id INT, 
    current_state VARCHAR,
    development_potential_id INT, 
    efficiency_estimation_id INT, 
    modification_date TIMESTAMP,
    app_instance_id INT 
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_subdivision;
CREATE TABLE sttgaz.stage_webtutor_subdivision(
    id BIGINT, 
	created DATETIME,
	modified DATETIME,
    xml_id BIGINT, 
    code VARCHAR(100),
    name VARCHAR(500),
    org_id BIGINT, 
    is_disbanded BOOLEAN,
    place_id BIGINT,
    region_id BIGINT,
    formed_date DATETIME,
    is_faculty BOOLEAN,
    comment VARCHAR(1000),
    creator_id BIGINT,
    xml_creation_date DATETIME,
    modificator_id BIGINT,
    xml_modification_date DATETIME,
    type_dc VARCHAR(100),
    site_diler VARCHAR(500),
    tel VARCHAR(500),
    street VARCHAR(1000),
    id_stoyanki VARCHAR(500),
    f_20u4 VARCHAR(500),
    id_plowadki VARCHAR(500),
    email VARCHAR(500),
    gps VARCHAR(500),
    skidki VARCHAR(500),
    doc_obor VARCHAR(500),
    f_4xwo VARCHAR(500),
    f_nliz VARCHAR(500),
    f_so4s VARCHAR(500),
    f_postpril1 VARCHAR(500),
    f_dilerskidki VARCHAR(500),
    plan_name VARCHAR(500),
    plan_value VARCHAR(500)
);


DROP TABLE IF EXISTS sttgaz.stage_webtutor_subdivisions;
CREATE TABLE sttgaz.stage_webtutor_subdivisions (
	id bigint NOT NULL,
	code varchar(900),
	name varchar(900),
	org_id bigint,
	parent_object_id bigint,
	is_disbanded boolean,
	place_id bigint,
	cost_center_id bigint,
	modification_date datetime,
	region_id bigint,
	is_faculty boolean
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_orgs;
CREATE TABLE sttgaz.stage_webtutor_orgs (
	id bigint NOT NULL,
	code varchar(1000),
	name varchar(900),
	disp_name varchar(900),
	account_id bigint,
	modification_date datetime,
	app_instance_id varchar(1000),
	kpi_profile_id bigint,
	bonus_profile_id bigint,
	place_id bigint,
	region_id bigint,
	tags varchar(1000)
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_regions;
CREATE TABLE sttgaz.stage_webtutor_regions (
	id bigint NOT NULL,
	code varchar(900),
	name varchar(900),
	modification_date datetime
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_places;
CREATE TABLE sttgaz.stage_webtutor_places (
	id bigint NOT NULL,
	code varchar(1000),
	name varchar(900),
	user_access_role varchar(1000),
	user_group_id bigint,
	modification_date datetime,
	region_id bigint,
	timezone_id bigint
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_collaborators;
CREATE TABLE sttgaz.stage_webtutor_collaborators (
	id bigint NOT NULL,
	code varchar(900),
	fullname varchar(900),
	email varchar(900),
	phone varchar(1000),
	mobile_phone varchar(1000),
	birth_date datetime,
	sex varchar(1000),
	position_id bigint,
	position_name varchar(1000),
	position_parent_id bigint,
	position_parent_name varchar(900),
	org_id bigint,
	org_name varchar(900),
	place_id bigint,
	region_id bigint,
	role_id varchar(1000),
	is_candidate boolean,
	candidate_status_type_id bigint,
	is_outstaff boolean,
	is_dismiss boolean,
	position_date datetime,
	hire_date datetime,
	dismiss_date datetime,
	current_state varchar(900),
	modification_date datetime
);