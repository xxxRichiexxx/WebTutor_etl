WITH
	sq1 AS
	(
	SELECT
		c.position_parent_id AS subdivision_id,
		COUNT(c.id) AS "МП. Факт"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE 'менедж%продаж%авто%'
		AND current_state IS NULL
	GROUP BY c.position_parent_id	
	),
	sq2 AS
	(
	SELECT
		c.position_parent_id AS subdivision_id,
		COUNT(c.id) AS "СтМОП. Факт"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE 'ст%менедж%продаж%'
		AND current_state IS NULL
	GROUP BY c.position_parent_id	
	),
	sq3 AS
	(
	SELECT
		c.position_parent_id AS subdivision_id,
		COUNT(c.id) AS "ЭКорП. Факт"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE '%корпорат%продаж%'
		AND current_state IS NULL
	GROUP BY c.position_parent_id	
	),
	sq4 AS
	(
	SELECT
		c.position_parent_id AS subdivision_id,
		COUNT(c.id) AS "РОП. Факт"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE (c.position_name iLIKE 'РОП' OR position_name LIKE '%РОП' OR position_name LIKE 'РОП%' OR  position_name iLIKE '%рук%отдел%продаж%')
		AND current_state IS NULL
	GROUP BY c.position_parent_id	
	),
	sq5 AS
	(
	SELECT
		c.position_parent_id AS subdivision_id,
		COUNT(c.id) AS "Маркетолог. Факт"
	FROM sttgaz.dds_webtutor_collaborators AS c
	WHERE c.position_name iLIKE '%маркетолог%'
		AND current_state IS NULL
	GROUP BY c.position_parent_id	
	)
SELECT
	o.name AS org_name_1,
	o.disp_name AS org_name_2,
	s.id AS subdivision_id,
	s.name AS sub_name,
	p.plan_name,
	p.plan_value,
	s.type_dc, 
	st."Менеджер по продажам новых автомобилей (МП)" AS "Менеджер по продажам новых автомобилей (МП). План",
	sq1."МП. Факт",
	st."Cтарший менеджер по продажам (СтМОП)" AS "Cтарший менеджер по продажам (СтМОП). План" ,
	sq2."СтМОП. Факт",
	st."Эксперт по корпоративным продажам (ЭКорП)" AS "Эксперт по корпоративным продажам (ЭКорП). План",
	sq3."ЭКорП. Факт",
	st."Руководитель отдела продаж (РОП)" AS "Руководитель отдела продаж (РОП). План",
	sq4."РОП. Факт",
	sq5."Маркетолог. Факт"
FROM sttgaz.dds_webtutor_subdivision AS s
LEFT JOIN sttgaz.dds_webtutor_plans AS p 
	ON s.id = p.subdivision_id
LEFT JOIN sttgaz.dds_webtutor_standart AS st 
	ON s.type_dc::varchar = st.type_dc::varchar
LEFT JOIN sttgaz.dds_webtutor_orgs AS o 
	ON s.org_id = o.id
LEFT JOIN sq1
	ON s.subdivision_id = sq1.subdivision_id
LEFT JOIN sq2
	ON s.subdivision_id = sq2.subdivision_id
LEFT JOIN sq3
	ON s.subdivision_id = sq3.subdivision_id
LEFT JOIN sq4
	ON s.subdivision_id = sq4.subdivision_id
LEFT JOIN sq5
	ON s.subdivision_id = sq5.subdivision_id
WHERE o.name = 'Авторитэйл М'






WITH 
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
	)
SELECT
	subdivision_id,
	"Год",
	MAX("Прирост") OVER (PARTITION BY subdivision_id ORDER BY "Год") - MAX("Убывание") OVER (PARTITION BY subdivision_id ORDER BY "Год") AS "Всего"
FROM sq3
ORDER BY subdivision_id DESC, "Год"