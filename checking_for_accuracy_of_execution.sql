INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'dm_webtutor_personal_stat',
	'checking_for_accuracy_of_execution',
	NOW(),
	"Управление" = 'Управление региональных продаж ЦЕНТР'
	AND org_name = 'АвтоГрафф'
	AND "Регион" = 'Волго-Вятский регион LCV-продажи'
	AND "Город" = 'Киров' 
	AND "Код" = '111-01-02'
	AND type_dc = 7
	AND plan_value = 241
	AND "Менеджер по продажам новых автомобилей (МП). План" = 2
	AND "МП. Факт" = 2
	AND "Cтарший менеджер по продажам (СтМОП). План" = 1
	AND "СтМОП. Факт" = 1
	AND "Эксперт по корпоративным продажам (ЭКорП). План" = 0
	AND "ЭКорП. Факт" = 0
	AND "Руководитель отдела продаж (РОП). План" = 	1
	AND "РОП. Факт" = 1
	AND "Маркетолог. Факт" = 1	
FROM sttgaz.dm_webtutor_personal_stat
WHERE subdivision_name = 'АВТОЦЕНТРГАЗ БЦР-АВТОКОМ' AND "Дата" = '2022-12-01';