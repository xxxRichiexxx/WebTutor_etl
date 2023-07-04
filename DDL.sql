-------------------------------------------STAGE-----------------------------------------

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
	modification_date datetime,
	place_id bigint,
	region_id bigint
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
	modification_date datetime,
	region_id bigint,
	timezone_id bigint,
	parent_object_id bigint
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
	modification_date datetime,
	web_banned boolean
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_positions;
CREATE TABLE sttgaz.stage_webtutor_positions (
	id bigint NOT NULL,
	code varchar(900),
	"name" varchar(1000),
	org_id bigint,
	parent_object_id bigint,
	position_common_id bigint,
	modification_date datetime
);

DROP TABLE IF EXISTS sttgaz.stage_webtutor_position_commons;
CREATE TABLE sttgaz.stage_webtutor_position_commons (
	id bigint NOT NULL,
	code varchar(900),
	"name" varchar(1000),
	position_familys bigint,
	modification_date datetime
);

--------------------------------------DDS-----------------------------------------


DROP TABLE IF EXISTS sttgaz.dds_webtutor_plans;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_collaborators;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_subdivision;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_positions;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_position_commons;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_orgs;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_places;
DROP TABLE IF EXISTS sttgaz.dds_webtutor_regions;



CREATE TABLE sttgaz.dds_webtutor_regions (
	"id" AUTO_INCREMENT PRIMARY KEY,
	region_id bigint NOT NULL,
	code varchar(900),
	name varchar(900),
	modification_date datetime
);

CREATE TABLE sttgaz.dds_webtutor_places (
	"id" AUTO_INCREMENT PRIMARY KEY,
	place_id bigint NOT NULL,
	code varchar(1000),
	name varchar(900),
	modification_date datetime,
	region_id bigint REFERENCES sttgaz.dds_webtutor_regions(id),
	timezone_id bigint,
	parent_object_id bigint
);


CREATE TABLE sttgaz.dds_webtutor_orgs (
	"id" AUTO_INCREMENT PRIMARY KEY,
	org_id bigint NOT NULL,
	code varchar(1000),
	name varchar(900),
	disp_name varchar(900),
	modification_date datetime,
	place_id bigint REFERENCES sttgaz.dds_webtutor_places(id),
	region_id bigint REFERENCES sttgaz.dds_webtutor_regions(id)
);


CREATE TABLE sttgaz.dds_webtutor_subdivision(
	"id" AUTO_INCREMENT PRIMARY KEY,
    subdivision_id BIGINT, 
    code VARCHAR(100),
    name VARCHAR(500),
    org_id bigint REFERENCES sttgaz.dds_webtutor_orgs(id),
    is_disbanded BOOLEAN,
    place_id bigint REFERENCES sttgaz.dds_webtutor_places(id),
    region_id bigint REFERENCES sttgaz.dds_webtutor_regions(id),
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
    f_dilerskidki VARCHAR(500)
);

CREATE TABLE sttgaz.dds_webtutor_position_commons (
	"id" AUTO_INCREMENT PRIMARY KEY,
	position_common_id bigint NOT NULL,
	code varchar(900),
	"name" varchar(1000),
	position_familys bigint,
	modification_date datetime
);


CREATE TABLE sttgaz.dds_webtutor_positions (
	"id" AUTO_INCREMENT PRIMARY KEY,
	position_id bigint NOT NULL,
	code varchar(900),
	"name" varchar(1000),
	org_id bigint REFERENCES sttgaz.dds_webtutor_orgs(id),
	parent_object_id bigint,
	position_common_id bigint REFERENCES sttgaz.dds_webtutor_position_commons(id),
	modification_date datetime
);

CREATE TABLE sttgaz.dds_webtutor_collaborators (
	"id" AUTO_INCREMENT PRIMARY KEY,
	collaborator_id bigint NOT NULL,
	code varchar(900),
	fullname varchar(900),
	email varchar(900),
	phone varchar(1000),
	mobile_phone varchar(1000),
	birth_date datetime,
	sex varchar(1000),
	position_id bigint REFERENCES sttgaz.dds_webtutor_positions(id),
	position_parent_id bigint,
	org_id bigint REFERENCES sttgaz.dds_webtutor_orgs(id),
	place_id bigint  REFERENCES sttgaz.dds_webtutor_places(id),
	region_id bigint REFERENCES sttgaz.dds_webtutor_regions(id),
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


CREATE TABLE sttgaz.dds_webtutor_plans (
	"id" AUTO_INCREMENT PRIMARY KEY,
	subdivision_id BIGINT REFERENCES sttgaz.dds_webtutor_subdivision(id),
    plan_name VARCHAR(500),
    plan_value VARCHAR(500)	
);

DROP TABLE IF EXISTS sttgaz.dds_webtutor_standart;
CREATE TABLE sttgaz.dds_webtutor_standart(
	"id" AUTO_INCREMENT PRIMARY KEY,
	plan VARCHAR(500),
	type_dc INT,
	"Менеджер по продажам новых автомобилей (МП)" VARCHAR(500),
	"Cтарший менеджер по продажам (СтМОП)" VARCHAR(500),
	"Эксперт по корпоративным продажам (ЭКорП)" VARCHAR(500),
	"Руководитель отдела продаж (РОП)" VARCHAR(500)
);

INSERT INTO sttgaz.dds_webtutor_standart
	(
	plan,
	type_dc,
	"Менеджер по продажам новых автомобилей (МП)",
	"Cтарший менеджер по продажам (СтМОП)",
	"Эксперт по корпоративным продажам (ЭКорП)",
	"Руководитель отдела продаж (РОП)"
	)
SELECT 'Больше 1000', 1,  7, 1, 2, 1
UNION
SELECT '855-999', 2,      6, 1, 1, 1
UNION
SELECT '700-849', 3,      5, 1, 1, 1
UNION
SELECT '550-699', 4,      4, 1, 1, 1
UNION
SELECT '400-549', 5,      4, 1, 1, 1
UNION
SELECT '250-399', 6,      3, 1, 1, 1
UNION
SELECT '100-249', 7,      2, 1, 1, 1
UNION
SELECT '50-99', 8,        2, 0, 0, 1
UNION
SELECT 'Менее 50', 9,     1, 0, 0, 1;




