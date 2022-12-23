SELECT  data.query('(/subdivision/code)[1]')
FROM    subdivision;


WITH 
sq AS (
	SELECT   data.value('(/subdivision/id)[1]', 'bigint') AS id
	        ,data.value('(/subdivision/code)[1]', 'varchar(100)') AS code
	        ,data.value('(/subdivision/name)[1]', 'varchar(500)') AS name
	        ,data.value('(/subdivision/org_id)[1]', 'bigint') AS org_id
	        ,data.value('(/subdivision/is_disbanded)[1]', 'bit') AS is_disbanded
	        ,data.value('(/subdivision/place_id)[1]', 'bigint') AS place_id
	        ,data.value('(/subdivision/region_id)[1]', 'bigint') AS region_id
	        ,data.value('(/subdivision/formed_date)[1]', 'datetime') AS formed_date
	        ,data.value('(/subdivision/is_faculty)[1]', 'bit') AS is_faculty
	        ,data.value('(/subdivision/comment)[1]', 'varchar(1000)') AS comment
	        ,data.value('(/subdivision/doc_info/creation/user_id)[1]','bigint') AS creator_id
	        ,data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime') AS creation_date
	        ,data.value('(/subdivision/doc_info/modification/user_id)[1]','bigint') AS modificator_id
	        ,data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime') AS modification_date
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="type_dc"]/value)[1]', 'varchar(100)') AS type_dc
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="site_diler"]/value)[1]', 'varchar(500)') AS site_diler
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="tel"]/value)[1]', 'varchar(500)') AS tel
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="street"]/value)[1]', 'varchar(1000)') AS street
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="id_stoyanki"]/value)[1]', 'varchar(500)') AS id_stoyanki
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="f_20u4"]/value)[1]', 'varchar(500)') AS f_20u4
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="id_plowadki"]/value)[1]', 'varchar(500)') AS id_plowadki
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="email"]/value)[1]', 'varchar(500)') AS email
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="gps"]/value)[1]', 'varchar(500)') AS gps
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="skidki"]/value)[1]', 'varchar(500)') AS skidki
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="doc_obor"]/value)[1]', 'varchar(500)') AS doc_obor
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="f_4xwo"]/value)[1]', 'varchar(500)') AS f_4xwo
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="f_nliz"]/value)[1]', 'varchar(500)') AS f_nliz
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="f_so4s"]/value)[1]', 'varchar(500)') AS f_so4s
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="f_postpril1"]/value)[1]', 'varchar(500)') AS f_postpril1
	        ,data.value('(/subdivision/custom_elems/custom_elem[name="f_dilerskidki"]/value)[1]', 'varchar(500)') AS f_dilerskidki
		    ,Table1.field.query('name').value('.', 'varchar(500)') AS plan_name
		    ,Table1.field.value('(./value)[1]', 'varchar(500)') AS plan_value
FROM    subdivision
        CROSS APPLY subdivision.data.nodes('(/subdivision/custom_elems/custom_elem)') AS Table1(field)
)
SELECT *
FROM sq
WHERE plan_name LIKE '%plan%'

SELECT   data.value('(/subdivision/id)[1]','bigint') AS id
        ,Table1.field.query('name').value('.', 'varchar(100)') AS nodes_name
        ,Table1.field.value('(./value)[1]', 'varchar(100)') AS nodes_value
FROM    subdivision
        CROSS APPLY subdivision.data.nodes('(/subdivision/custom_elems/custom_elem)') AS Table1(field)