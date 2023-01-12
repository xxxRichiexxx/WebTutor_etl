CREATE OR REPLACE VIEW sttgaz.dm_webtutor_personal_stat AS
WITH
	------------Динамика количества менеджеров по продажам--------------
	sq1 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE 'менедж%продаж%авто%'
	),
	sq2 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE 'менедж%продаж%авто%'
		AND dismiss_date IS NOT NULL
	),
	sq3 AS(
	SELECT
		COALESCE(sq1.subdivision_id, sq2.subdivision_id) 																	AS subdivision_id,
		COALESCE(sq1."Дата трудоустройства", sq2."Дата увольнения") 														AS "Дата",
		COALESCE("Прирост",0) 																								AS "Прирост",
		COALESCE("Убывание",0) 																								AS "Убывание"
	FROM sq1
	FULL JOIN sq2
		ON sq1.subdivision_id = sq2.subdivision_id AND sq1."Дата трудоустройства" = sq2."Дата увольнения"
	),
				-----"МП. Факт"--------
	sq4 AS(
	SELECT
		subdivision_id,
		"Дата",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Дата") - 
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата") 												AS "МП. Факт"
	FROM sq3
	),
	------------Динамика количества старших менеджеров по продажам--------------
	sq5 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE 'ст%менедж%продаж%'
	),
	sq6 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE 'ст%менедж%продаж%'
		AND dismiss_date IS NOT NULL
	),
	sq7 AS(
	SELECT
		COALESCE(sq5.subdivision_id, sq6.subdivision_id) 																	AS subdivision_id,
		COALESCE(sq5."Дата трудоустройства", sq6."Дата увольнения") 														AS "Дата",
		COALESCE("Прирост",0) 																								AS "Прирост",
		COALESCE("Убывание",0) 																								AS "Убывание"
	FROM sq5
	FULL JOIN sq6
		ON sq5.subdivision_id = sq6.subdivision_id AND sq5."Дата трудоустройства" = sq6."Дата увольнения"
	),
				-----"СтМОП. Факт"--------
	sq8 AS(
	SELECT
		subdivision_id,
		"Дата",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата") 												AS "СтМОП. Факт"
	FROM sq7
	),
	------------Динамика количества экспертов по корпоративным продажам--------------
	sq9 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE '%корпорат%продаж%'
	),
	sq10 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE '%корпорат%продаж%'
		AND dismiss_date IS NOT NULL
	),
	sq11 AS(
	SELECT
		COALESCE(sq9.subdivision_id, sq10.subdivision_id) 																	AS subdivision_id,
		COALESCE(sq9."Дата трудоустройства", sq10."Дата увольнения") 														AS "Дата",
		COALESCE("Прирост",0) 																								AS "Прирост",
		COALESCE("Убывание",0) 																								AS "Убывание"
	FROM sq9
	FULL JOIN sq10
		ON sq9.subdivision_id = sq10.subdivision_id AND sq9."Дата трудоустройства" = sq10."Дата увольнения"
	),
				-----"ЭКорП. Факт"--------
	sq12 AS(
	SELECT
		subdivision_id,
		"Дата",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата") 												AS "ЭКорП. Факт"
	FROM sq11
	),
	------------Динамика количества руководителей отделов продаж--------------
	sq13 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE (c.position_name iLIKE 'РОП' OR position_name LIKE '%РОП' OR position_name LIKE 'РОП%' OR  position_name iLIKE '%рук%отдел%продаж%')
	),
	sq14 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE (c.position_name iLIKE 'РОП' OR position_name LIKE '%РОП' OR position_name LIKE 'РОП%' OR  position_name iLIKE '%рук%отдел%продаж%')
		AND dismiss_date IS NOT NULL
	),
	sq15 AS(
	SELECT
		COALESCE(sq13.subdivision_id, sq14.subdivision_id) 										    						AS subdivision_id,
		COALESCE(sq13."Дата трудоустройства", sq14."Дата увольнения") 							    						AS "Дата",
		COALESCE("Прирост",0) 																	    						AS "Прирост",
		COALESCE("Убывание",0) 																	    						AS "Убывание"
	FROM sq13
	FULL JOIN sq14
		ON sq13.subdivision_id = sq14.subdivision_id AND sq13."Дата трудоустройства" = sq14."Дата увольнения"
	),
				-----"РОП. Факт"--------
	sq16 AS(
	SELECT
		subdivision_id,
		"Дата",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата") 					    						AS "РОП. Факт"
	FROM sq15
	),
	------------Динамика количества маркетологов--------------
	sq17 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата трудоустройства",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE '%маркетолог%'
	),
	sq18 AS(
	SELECT DISTINCT
		c.position_parent_id 																								AS subdivision_id,
		DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата увольнения",
		COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
	FROM sttgaz.dds_webtutor_collaborators 																					AS c
	WHERE c.position_name iLIKE '%маркетолог%'
		AND dismiss_date IS NOT NULL
	),
	sq19 AS(
	SELECT
		COALESCE(sq17.subdivision_id, sq18.subdivision_id) 																	AS subdivision_id,
		COALESCE(sq17."Дата трудоустройства", sq18."Дата увольнения") 														AS "Дата",
		COALESCE("Прирост",0) 																								AS "Прирост",
		COALESCE("Убывание",0) 																								AS "Убывание"
	FROM sq17
	FULL JOIN sq18
		ON sq17.subdivision_id = sq18.subdivision_id AND sq17."Дата трудоустройства" = sq18."Дата увольнения"
	),
				-----"Маркетолог. Факт"--------
	sq20 AS(
	SELECT
		subdivision_id,
		"Дата",
		MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
			MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата") 												AS "Маркетолог. Факт"
	FROM sq19
	),
	----------------Диапазон дат (годов)-------------------------------
	years AS(
	SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date AS "Дата"
	FROM (SELECT '2000-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) as t
	TIMESERIES ts as '1 MONTH' OVER (ORDER BY t.tm)
	)
SELECT
	o.disp_name 																											AS org_name,
	s.id 																													AS subdivision_id,
	s.name																													AS subdivision_name,
	COALESCE(s.code, s.f_nliz)																								AS "Код",
	s.site_diler,
	r.name 																													AS "Управление",
	pl.name  																												AS "Город",
	s.f_4xwo																												AS "Регион",
	years."Дата",
	p.plan_name,
	p.plan_value,
	s.type_dc,
	st."Менеджер по продажам новых автомобилей (МП)" 																		AS "Менеджер по продажам новых автомобилей (МП). План",
	COALESCE(
		LAST_VALUE(sq4."МП. Факт" ignore nulls) OVER (PARTITION BY s.subdivision_id  ORDER BY years."Дата"),
		0) 																													AS "МП. Факт",
	st."Cтарший менеджер по продажам (СтМОП)" 																				AS "Cтарший менеджер по продажам (СтМОП). План",
	COALESCE(
		LAST_VALUE(sq8."СтМОП. Факт" ignore nulls) OVER (PARTITION BY s.subdivision_id  ORDER BY years."Дата"),
		0) 																													AS "СтМОП. Факт",
	st."Эксперт по корпоративным продажам (ЭКорП)" 																			AS "Эксперт по корпоративным продажам (ЭКорП). План",
	COALESCE(
		LAST_VALUE(sq12."ЭКорП. Факт" ignore nulls) OVER (PARTITION BY s.subdivision_id  ORDER BY years."Дата"),
		0) 																													AS "ЭКорП. Факт",
	st."Руководитель отдела продаж (РОП)" 																					AS "Руководитель отдела продаж (РОП). План",
	COALESCE(
		LAST_VALUE(sq16."РОП. Факт" ignore nulls) OVER (PARTITION BY s.subdivision_id  ORDER BY years."Дата"),
		0) 																													AS "РОП. Факт",
	COALESCE(
		LAST_VALUE(sq20."Маркетолог. Факт" ignore nulls) OVER (PARTITION BY s.subdivision_id  ORDER BY years."Дата"),
		0) 																													AS "Маркетолог. Факт"
FROM sttgaz.dds_webtutor_subdivision 	AS s
CROSS JOIN years
LEFT JOIN sttgaz.dds_webtutor_plans 	AS p 
	ON s.id = p.subdivision_id AND RIGHT(p.plan_name, 4)::INT = EXTRACT(YEAR FROM years."Дата")
LEFT JOIN sttgaz.dds_webtutor_standart 	AS st 
	ON s.type_dc::varchar = st.type_dc::varchar
LEFT JOIN sttgaz.dds_webtutor_orgs 		AS o 
	ON s.org_id = o.id
LEFT JOIN sttgaz.dds_webtutor_regions 	AS r 
	ON s.region_id = r.id
LEFT JOIN sttgaz.dds_webtutor_places 	AS pl
	ON s.place_id = pl.id
LEFT JOIN sq4
	ON s.subdivision_id = sq4.subdivision_id AND years."Дата" = sq4."Дата" 
LEFT JOIN sq8
	ON s.subdivision_id = sq8.subdivision_id AND years."Дата" = sq8."Дата" 
LEFT JOIN sq12
	ON s.subdivision_id = sq12.subdivision_id AND years."Дата" = sq12."Дата" 
LEFT JOIN sq16
	ON s.subdivision_id = sq16.subdivision_id AND years."Дата" = sq16."Дата" 
LEFT JOIN sq20
	ON s.subdivision_id = sq20.subdivision_id AND years."Дата" = sq20."Дата";