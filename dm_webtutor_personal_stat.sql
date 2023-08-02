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
		TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)
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
			s.f_4xwo																											AS "Регион + направление",
			pl2.name																											AS "Область",
			pl3.name																											AS "Регион",
			COALESCE(pl4.name, 	COALESCE(pl3.name, pl2.name))																	AS "Страна",
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
            COALESCE(MAX(sq10."Убывание") OVER (PARTITION BY s.subdivision_id ORDER BY years."Дата"), 0)                        AS  "Марк. Убывание"
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
		LEFT JOIN sttgaz.dds_webtutor_places 	AS pl2
			ON pl.parent_object_id = pl2.place_id
		LEFT JOIN sttgaz.dds_webtutor_places 	AS pl3
			ON pl2.parent_object_id = pl3.place_id
		LEFT JOIN sttgaz.dds_webtutor_places 	AS pl4
			ON pl3.parent_object_id = pl4.place_id 
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
			AND s.f_20u4 = 'LCV'
    )
SELECT
	org_name,
	subdivision_id,
	subdivision_name,
	"Код",
	site_diler,
	CASE
		WHEN "Управление" LIKE '%Управление региональных продаж%' THEN SUBSTRING("Управление", 32)
		ELSE "Управление"
	END 																													AS "Управление",
	"Город",
	"Регион + направление",
	"Область",
	"Регион",
	"Страна",
	"Дата",
	plan_name,
	plan_value,
	type_dc,
	"Менеджер по продажам новых автомобилей (МП). План",
	"МП. Прирост" - LAG("МП. Убывание", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                     		AS "МП. Факт",

	LAG("Менеджер по продажам новых автомобилей (МП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")		AS "МП. План предыдущий месяц",
	LAG("МП. Прирост", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
		LAG("МП. Убывание", 2, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                     					AS "МП. Факт предыдущий месяц",

	"Cтарший менеджер по продажам (СтМОП). План",
	"стМОП. Прирост" - LAG("стМОП. Убывание", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                		AS "СтМОП. Факт",

	LAG("Cтарший менеджер по продажам (СтМОП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")				AS "СтМОП. План предыдущий месяц",
	LAG("стМОП. Прирост", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
		LAG("стМОП. Убывание", 2, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                     				AS "СтМОП. Факт предыдущий месяц",

	"Эксперт по корпоративным продажам (ЭКорП). План",
	"ЭКорп. Прирост" - LAG("ЭКорп. Убывание", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")              		AS "ЭКорП. Факт",

	LAG("Эксперт по корпоративным продажам (ЭКорП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")			AS "ЭКорп. План предыдущий месяц",
	LAG("ЭКорп. Прирост", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
		LAG("ЭКорп. Убывание", 2, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                     				AS "ЭКорп. Факт предыдущий месяц",

	"Руководитель отдела продаж (РОП). План",
	"РОП. Прирост" - LAG("РОП. Убывание", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                    		AS "РОП. Факт",

	LAG("Руководитель отдела продаж (РОП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")					AS "РОП. План предыдущий месяц",
	LAG("РОП. Прирост", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
		LAG("РОП. Убывание", 2, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                     					AS "РОП. Факт предыдущий месяц",

	"Марк. Прирост" - LAG("Марк. Убывание", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                  		AS "Маркетолог. Факт",

	LAG("Марк. Прирост", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") -
		LAG("Марк. Убывание", 2, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")                     					AS "Марк. Факт предыдущий месяц",

	"Менеджер по продажам новых автомобилей (МП). План" +
	-- "Эксперт по корпоративным продажам (ЭКорП). План" +
	"Руководитель отдела продаж (РОП). План" 																				AS "Торг. персонала всего. План",

	"МП. Факт" + "СтМОП. Факт" + "ЭКорП. Факт" + "РОП. Факт" 					 											AS "Торг. персонала всего. Факт",

	LAG("Менеджер по продажам новых автомобилей (МП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") +
	-- LAG("Эксперт по корпоративным продажам (ЭКорП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") +
	LAG("Руководитель отдела продаж (РОП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")					AS "Торг. персонала всего. План. Предыдущий месяц",

	"МП. Факт предыдущий месяц" + "СтМОП. Факт предыдущий месяц" + "ЭКорп. Факт предыдущий месяц" +
		"РОП. Факт предыдущий месяц" 								 														AS "Торг. персонала всего. Факт. Предыдущий месяц",

	CASE
		WHEN "МП. Факт" + "СтМОП. Факт" - "Менеджер по продажам новых автомобилей (МП). План" > 0 THEN 0
		ELSE "МП. Факт" + "СтМОП. Факт" - "Менеджер по продажам новых автомобилей (МП). План"
	END 																													AS "Нехватка МП",

	CASE
		WHEN "МП. Факт предыдущий месяц" + 
			"СтМОП. Факт предыдущий месяц" - 
			LAG("Менеджер по продажам новых автомобилей (МП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") > 0 THEN 0
		ELSE "МП. Факт предыдущий месяц" + 
			"СтМОП. Факт предыдущий месяц" - 
			LAG("Менеджер по продажам новых автомобилей (МП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")
	END 																													AS "Нехватка МП. Предыдущий месяц",

	CASE
		WHEN "ЭКорП. Факт" - "Эксперт по корпоративным продажам (ЭКорП). План" > 0 THEN 0
		ELSE "ЭКорП. Факт" - "Эксперт по корпоративным продажам (ЭКорП). План"
	END 																													AS "Нехватка ЭКорП",

	CASE
		WHEN "ЭКорп. Факт предыдущий месяц" - LAG("Эксперт по корпоративным продажам (ЭКорП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") > 0 THEN 0
		ELSE "ЭКорп. Факт предыдущий месяц" - LAG("Эксперт по корпоративным продажам (ЭКорП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")	
	END 																													AS "Нехватка ЭКорП. Предыдущий месяц",

	CASE
		WHEN "РОП. Факт" - "Руководитель отдела продаж (РОП). План" > 0 THEN 0
		ELSE "РОП. Факт" - "Руководитель отдела продаж (РОП). План"
	END 																													AS "Нехватка РОП",

	CASE
		WHEN "РОП. Факт предыдущий месяц" - LAG("Руководитель отдела продаж (РОП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата") > 0 THEN 0
		ELSE "РОП. Факт предыдущий месяц" - LAG("Руководитель отдела продаж (РОП). План", 1, 0) OVER (PARTITION BY subdivision_id ORDER BY "Дата")
	END 																													AS "Нехватка РОП. Предыдущий месяц",

	CASE
		WHEN "Торг. персонала всего. Факт" - "Торг. персонала всего. План" > 0 THEN 0
		ELSE "Торг. персонала всего. Факт" - "Торг. персонала всего. План"
	END																														AS "Нехватка всего",

	CASE
		WHEN "Торг. персонала всего. Факт. Предыдущий месяц" - "Торг. персонала всего. План. Предыдущий месяц" > 0 THEN 0 
		ELSE "Торг. персонала всего. Факт. Предыдущий месяц" - "Торг. персонала всего. План. Предыдущий месяц"
	END																														AS "Нехватка всего. Предыдущий месяц",

	CASE
		WHEN "Нехватка всего" < 0 THEN 1
		ELSE 0
	END																														AS "Нехватка персонала в текущем месяце (да/нет)",

	CASE
		WHEN "Нехватка всего. Предыдущий месяц" < 0 THEN 1
		ELSE 0
	END 																													AS "Нехватка персонала в предыдущем месяце (да/нет)"
FROM dm_prepare;