CREATE OR REPLACE VIEW sttgaz.dm_webtutor_personal_stat AS
WITH
	------------Динамика трудоустройства менеджеров по продажам--------------
	sq1 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Менеджер по продажам автомобилей'
	),
	sq2 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 													
		AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Менеджер по продажам автомобилей'
			AND dismiss_date IS NOT NULL
	),
	------------Динамика трудоустройства старших менеджеров по продажам--------------
	sq3 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Старший менеджер по продажам автомобилей'
	),
	sq4 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Старший менеджер по продажам автомобилей'
			AND dismiss_date IS NOT NULL
	),
	------------Динамика трудоустройства экспертов по корпоративным продажам--------------
	sq5 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Менеджер по корпоративным продажам'
	),
	sq6 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Менеджер по корпоративным продажам'
			AND dismiss_date IS NOT NULL
	),
	------------Динамика трудоустройства руководителей отделов продаж--------------
	sq7 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'РОП'
	),
	sq8 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'РОП'
			AND dismiss_date IS NOT NULL
	),
	------------Динамика трудоустройства маркетологов--------------
	sq9 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))::date														AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', COALESCE(hire_date, dismiss_date))) 		AS "Прирост"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Маркетолог ДЦ'
	),
	sq10 AS(
		SELECT DISTINCT
			c.position_parent_id 																								AS subdivision_id,
			DATE_TRUNC('MONTH', dismiss_date)::date																				AS "Дата",
			COUNT(c.id) OVER (PARTITION BY subdivision_id ORDER BY DATE_TRUNC('MONTH', dismiss_date)) 							AS "Убывание"
		FROM sttgaz.dds_webtutor_collaborators 																					AS c
		JOIN sttgaz.dds_webtutor_positions 																						AS p
			ON c.position_id = p.id 
		JOIN sttgaz.dds_webtutor_position_commons 																				AS pc
			ON p.position_common_id = pc.id
		WHERE pc.name = 'Маркетолог ДЦ'
			AND dismiss_date IS NOT NULL
	),
	----------------Диапазон дат (годов)-------------------------------
	years AS(
		SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date AS "Дата"
		FROM (SELECT '2000-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) as t
		TIMESERIES ts as '1 MONTH' OVER (ORDER BY t.tm)
	),
    dm_prepare AS(
		SELECT
			o.disp_name 																									    AS org_name,
			s.id 																												AS subdivision_id,
			s.name																												AS subdivision_name,
			COALESCE(s.code, s.f_nliz)																							AS "Код",
			s.site_diler,
			r.name 																												AS "Управление",
			pl.name  																											AS "Город",
			s.f_4xwo																											AS "Регион",
			years."Дата",
			p.plan_name,
			p.plan_value,
			s.type_dc,
			st."Менеджер по продажам новых автомобилей (МП)" 																	AS "Менеджер по продажам новых автомобилей (МП). План",
            COALESCE(MAX(sq1."Прирост") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                          AS  "МП. Прирост",  
            COALESCE(MAX(sq2."Убывание") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                         AS  "МП. Убывание",
            st."Cтарший менеджер по продажам (СтМОП)" 																		    AS "Cтарший менеджер по продажам (СтМОП). План",
            COALESCE(MAX(sq3."Прирост") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                          AS  "стМОП. Прирост",  
            COALESCE(MAX(sq4."Убывание") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                         AS  "стМОП. Убывание",
            st."Эксперт по корпоративным продажам (ЭКорП)" 																	    AS "Эксперт по корпоративным продажам (ЭКорП). План",
            COALESCE(MAX(sq5."Прирост") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                          AS  "ЭКорп. Прирост",  
            COALESCE(MAX(sq6."Убывание") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                         AS  "ЭКорп. Убывание",
            st."Руководитель отдела продаж (РОП)" 																			    AS "Руководитель отдела продаж (РОП). План",
            COALESCE(MAX(sq7."Прирост") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                          AS  "РОП. Прирост",  
            COALESCE(MAX(sq8."Убывание") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                         AS  "РОП. Убывание",
            COALESCE(MAX(sq9."Прирост") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                          AS  "Марк. Прирост",  
            COALESCE(MAX(sq10."Убывание") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                         AS  "Марк. Убывание"
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
		LEFT JOIN sq1
			ON s.subdivision_id = sq1.subdivision_id AND years."Дата" = sq1."Дата" 
		LEFT JOIN sq2
			ON s.subdivision_id = sq2.subdivision_id AND years."Дата" = sq2."Дата" 
		LEFT JOIN sq3
			ON s.subdivision_id = sq3.subdivision_id AND years."Дата" = sq3."Дата" 
		LEFT JOIN sq4
			ON s.subdivision_id = sq4.subdivision_id AND years."Дата" = sq4."Дата" 
		LEFT JOIN sq5
			ON s.subdivision_id = sq5.subdivision_id AND years."Дата" = sq5."Дата"
		LEFT JOIN sq6
			ON s.subdivision_id = sq6.subdivision_id AND years."Дата" = sq6."Дата"
		LEFT JOIN sq7
			ON s.subdivision_id = sq7.subdivision_id AND years."Дата" = sq7."Дата"
		LEFT JOIN sq8
			ON s.subdivision_id = sq8.subdivision_id AND years."Дата" = sq8."Дата"
		LEFT JOIN sq9
			ON s.subdivision_id = sq9.subdivision_id AND years."Дата" = sq9."Дата"
		LEFT JOIN sq10
			ON s.subdivision_id = sq10.subdivision_id AND years."Дата" = sq10."Дата"
		WHERE s.is_disbanded = FALSE
    )
SELECT
	org_name,
	subdivision_id,
	subdivision_name,
	"Код",
	site_diler,
	"Управление",
	"Город",
	"Регион",
	"Дата",
	plan_name,
	plan_value,
	type_dc,
    "Менеджер по продажам новых автомобилей (МП). План",
    "МП. Прирост" - COALESCE(LAG("МП. Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата"),0)                      AS "МП. Факт",
    "Cтарший менеджер по продажам (СтМОП). План",
    "стМОП. Прирост" - COALESCE(LAG("стМОП. Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата"),0)                AS "СтМОП. Факт",
    "Эксперт по корпоративным продажам (ЭКорП). План",
    "ЭКорп. Прирост" - COALESCE(LAG("ЭКорп. Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата"),0)                AS "ЭКорП. Факт",
    "Руководитель отдела продаж (РОП). План",
    "РОП. Прирост" - COALESCE(LAG("РОП. Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата"),0)                    AS "РОП. Факт",
    "Марк. Прирост" - COALESCE(LAG("Марк. Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Дата"),0)                  AS "Маркетолог. Факт",
	"МП. Факт" + "СтМОП. Факт" + "ЭКорП. Факт" + "РОП. Факт" + "Маркетолог. Факт" 											AS "Торг. персонала всего",
	CASE
		WHEN "МП. Факт" - "Менеджер по продажам новых автомобилей (МП). План" > 0 THEN 0
		ELSE "МП. Факт" - "Менеджер по продажам новых автомобилей (МП). План"
	END 																													AS "Нехватка МП",
	CASE
		WHEN "СтМОП. Факт" - "Cтарший менеджер по продажам (СтМОП). План" > 0 THEN 0
		ELSE "СтМОП. Факт" - "Cтарший менеджер по продажам (СтМОП). План"
	END 																													AS "Нехватка СтМОП",
	CASE
		WHEN "ЭКорП. Факт" - "Эксперт по корпоративным продажам (ЭКорП). План" > 0 THEN 0
		ELSE "ЭКорП. Факт" - "Эксперт по корпоративным продажам (ЭКорП). План"
	END 																													AS "Нехватка ЭКорП",
	CASE
		WHEN "РОП. Факт" - "Руководитель отдела продаж (РОП). План" > 0 THEN 0
		ELSE "РОП. Факт" - "Руководитель отдела продаж (РОП). План"
	END 																													AS "Нехватка РОП",
	"Нехватка МП" + "Нехватка СтМОП" + "Нехватка ЭКорП" + "Нехватка РОП" 													AS "Нехватка всего"
FROM dm_prepare;