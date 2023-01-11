WITH 
	------------Динамика количества менеджеров по продажам--------------
	sq1 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM hire_date) AS "Год трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM hire_date)) AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE 'менедж%продаж%авто%'
	),
	sq2 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM dismiss_date) AS "Год увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM dismiss_date)) AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE 'менедж%продаж%авто%'
		AND dismiss_date IS NOT NULL
	),
	sq3 AS(
	SELECT
		COALESCE(sq1.subdivision_id, sq2.subdivision_id) AS subdivision_id,
		COALESCE(sq1."Год трудоустройства", sq2."Год увольнения") AS "Год",
		COALESCE("Прирост",0) AS "Прирост",
		COALESCE("Убывание",0) AS "Убывание"
	FROM sq1
	FULL JOIN sq2
		ON sq1.subdivision_id = sq2.subdivision_id AND sq1."Год трудоустройства" = sq2."Год увольнения"
	),
	-----"МП. Факт"--------
	sq4 AS(
	SELECT
		subdivision_id,
		"Год",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Год") - 
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Год") AS "МП. Факт"
	FROM sq3
	ORDER BY subdivision_id DESC, "Год"
	),
	------------Динамика количества старших менеджеров по продажам--------------
	sq5 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM hire_date) AS "Год трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM hire_date)) AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE 'ст%менедж%продаж%'
	),
	sq6 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM dismiss_date) AS "Год увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM dismiss_date)) AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE 'ст%менедж%продаж%'
		AND dismiss_date IS NOT NULL
	),
	sq7 AS(
	SELECT
		COALESCE(sq5.subdivision_id, sq6.subdivision_id) AS subdivision_id,
		COALESCE(sq5."Год трудоустройства", sq6."Год увольнения") AS "Год",
		COALESCE("Прирост",0) AS "Прирост",
		COALESCE("Убывание",0) AS "Убывание"
	FROM sq5
	FULL JOIN sq6
		ON sq5.subdivision_id = sq6.subdivision_id AND sq5."Год трудоустройства" = sq6."Год увольнения"
	),
	-----"СтМОП. Факт"--------
	sq8 AS(
	SELECT
		subdivision_id,
		"Год",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Год") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Год") AS "СтМОП. Факт"
	FROM sq7
	ORDER BY subdivision_id DESC, "Год"
	),
	------------Динамика количества экспертов по корпоративным продажам--------------
	sq9 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM hire_date) AS "Год трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM hire_date)) AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE '%корпорат%продаж%'
	),
	sq10 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM dismiss_date) AS "Год увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM dismiss_date)) AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE '%корпорат%продаж%'
		AND dismiss_date IS NOT NULL
	),
	sq11 AS(
	SELECT
		COALESCE(sq9.subdivision_id, sq10.subdivision_id) AS subdivision_id,
		COALESCE(sq9."Год трудоустройства", sq10."Год увольнения") AS "Год",
		COALESCE("Прирост",0) AS "Прирост",
		COALESCE("Убывание",0) AS "Убывание"
	FROM sq9
	FULL JOIN sq10
		ON sq9.subdivision_id = sq10.subdivision_id AND sq9."Год трудоустройства" = sq10."Год увольнения"
	),
	-----"ЭКорП. Факт"--------
	sq12 AS(
	SELECT
		subdivision_id,
		"Год",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Год") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Год") AS "ЭКорП. Факт"
	FROM sq11
	ORDER BY subdivision_id DESC, "Год"
	),
	------------Динамика количества руководителей отделов продаж--------------
	sq13 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM hire_date) AS "Год трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM hire_date)) AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE (c.position_name iLIKE 'РОП' OR position_name LIKE '%РОП' OR position_name LIKE 'РОП%' OR  position_name iLIKE '%рук%отдел%продаж%')
	),
	sq14 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM dismiss_date) AS "Год увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM dismiss_date)) AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE (c.position_name iLIKE 'РОП' OR position_name LIKE '%РОП' OR position_name LIKE 'РОП%' OR  position_name iLIKE '%рук%отдел%продаж%')
		AND dismiss_date IS NOT NULL
	),
	sq15 AS(
	SELECT
		COALESCE(sq13.subdivision_id, sq14.subdivision_id) AS subdivision_id,
		COALESCE(sq13."Год трудоустройства", sq14."Год увольнения") AS "Год",
		COALESCE("Прирост",0) AS "Прирост",
		COALESCE("Убывание",0) AS "Убывание"
	FROM sq13
	FULL JOIN sq14
		ON sq13.subdivision_id = sq14.subdivision_id AND sq13."Год трудоустройства" = sq14."Год увольнения"
	),
	-----"РОП. Факт"--------
	sq16 AS(
	SELECT
		subdivision_id,
		"Год",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Год") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Год") AS "РОП. Факт"
	FROM sq15
	ORDER BY subdivision_id DESC, "Год"
	),
	------------Динамика количества маркетологов--------------
	sq17 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM hire_date) AS "Год трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM hire_date)) AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE '%маркетолог%'
	),
	sq18 AS(
	SELECT DISTINCT
		c.position_parent_id AS subdivision_id,
		EXTRACT(YEAR FROM dismiss_date) AS "Год увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY EXTRACT(YEAR FROM dismiss_date)) AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE '%маркетолог%'
		AND dismiss_date IS NOT NULL
	),
	sq19 AS(
	SELECT
		COALESCE(sq17.subdivision_id, sq18.subdivision_id) AS subdivision_id,
		COALESCE(sq17."Год трудоустройства", sq18."Год увольнения") AS "Год",
		COALESCE("Прирост",0) AS "Прирост",
		COALESCE("Убывание",0) AS "Убывание"
	FROM sq17
	FULL JOIN sq18
		ON sq17.subdivision_id = sq18.subdivision_id AND sq17."Год трудоустройства" = sq18."Год увольнения"
	),
	-----"Маркетолог. Факт"--------
	sq20 AS(
	SELECT
		subdivision_id,
		"Год",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Год") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Год") AS "Маркетолог. Факт"
	FROM sq19
	ORDER BY subdivision_id DESC, "Год"
	)
SELECT
	o.disp_name AS org_name,
	s.id AS subdivision_id,
	s.name AS sub_name,
	p.plan_name,
	p.plan_value,
	s.type_dc,
	st."Менеджер по продажам новых автомобилей (МП)" AS "Менеджер по продажам новых автомобилей (МП). План",
	MAX(sq4."МП. Факт") OVER (PARTITION BY s.subdivision_id  ORDER BY p.plan_name) AS "МП. Факт",
	st."Cтарший менеджер по продажам (СтМОП)" AS "Cтарший менеджер по продажам (СтМОП). План",
	MAX(sq8."СтМОП. Факт") OVER (PARTITION BY s.subdivision_id  ORDER BY p.plan_name) AS "СтМОП. Факт",
	st."Эксперт по корпоративным продажам (ЭКорП)" AS "Эксперт по корпоративным продажам (ЭКорП). План",
	MAX(sq12."ЭКорП. Факт") OVER (PARTITION BY s.subdivision_id  ORDER BY p.plan_name) AS "ЭКорП. Факт",
	st."Руководитель отдела продаж (РОП)" AS "Руководитель отдела продаж (РОП). План",
	MAX(sq16."РОП. Факт") OVER (PARTITION BY s.subdivision_id  ORDER BY p.plan_name) AS "РОП. Факт",
	MAX(sq20."Маркетолог. Факт") OVER (PARTITION BY s.subdivision_id  ORDER BY p.plan_name) AS "Маркетолог. Факт"
FROM sttgaz.dds_webtutor_subdivision AS s
INNER JOIN sttgaz.dds_webtutor_plans AS p 
	ON s.id = p.subdivision_id AND p.plan_name IS NOT NULL
LEFT JOIN sttgaz.dds_webtutor_standart AS st 
	ON s.type_dc::varchar = st.type_dc::varchar
LEFT JOIN sttgaz.dds_webtutor_orgs AS o 
	ON s.org_id = o.id
LEFT JOIN sq4
	ON s.subdivision_id = sq4.subdivision_id AND RIGHT(p.plan_name, 4)::INT = sq4."Год" 
LEFT JOIN sq8
	ON s.subdivision_id = sq8.subdivision_id AND RIGHT(p.plan_name, 4)::INT = sq8."Год" 
LEFT JOIN sq12
	ON s.subdivision_id = sq12.subdivision_id AND RIGHT(p.plan_name, 4)::INT = sq12."Год" 
LEFT JOIN sq16
	ON s.subdivision_id = sq16.subdivision_id AND RIGHT(p.plan_name, 4)::INT = sq16."Год" 	
LEFT JOIN sq20
	ON s.subdivision_id = sq20.subdivision_id AND RIGHT(p.plan_name, 4)::INT = sq20."Год"