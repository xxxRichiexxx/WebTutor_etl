WITH 
    sq1 AS (
        SELECT 	id
                ,created
                ,modified
                ,COALESCE(data.value('(/subdivision/id)[1]', 'bigint'), 0)                                              AS xml_id
                ,data.value('(/subdivision/code)[1]', 'varchar(100)')                                                   AS code
                ,data.value('(/subdivision/name)[1]', 'varchar(500)')                                                   AS name
                ,COALESCE(data.value('(/subdivision/org_id)[1]', 'bigint'), 0)                                          AS org_id
                ,data.value('(/subdivision/is_disbanded)[1]', 'bit')                                                    AS is_disbanded
                ,COALESCE(data.value('(/subdivision/place_id)[1]', 'bigint'), 0)                                        AS place_id
                ,COALESCE(data.value('(/subdivision/region_id)[1]', 'bigint'), 0)                                       AS region_id
                ,data.value('(/subdivision/formed_date)[1]', 'datetime')                                                AS formed_date
                ,data.value('(/subdivision/is_faculty)[1]', 'bit')                                                      AS is_faculty
                ,data.value('(/subdivision/comment)[1]', 'varchar(1000)')                                               AS comment
                ,COALESCE(data.value('(/subdivision/doc_info/creation/user_id)[1]','bigint'), 0)                        AS creator_id
                ,data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime')                                     AS xml_creation_date
                ,COALESCE(data.value('(/subdivision/doc_info/modification/user_id)[1]','bigint'), 0)                    AS modificator_id
                ,data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime')                                 AS xml_modification_date
                ,data.value('(/subdivision/custom_elems/custom_elem[name="type_dc"]/value)[1]', 'varchar(100)')         AS type_dc
                ,data.value('(/subdivision/custom_elems/custom_elem[name="site_diler"]/value)[1]', 'varchar(500)')      AS site_diler
                ,data.value('(/subdivision/custom_elems/custom_elem[name="tel"]/value)[1]', 'varchar(500)')             AS tel
                ,data.value('(/subdivision/custom_elems/custom_elem[name="street"]/value)[1]', 'varchar(1000)')         AS street
                ,data.value('(/subdivision/custom_elems/custom_elem[name="id_stoyanki"]/value)[1]', 'varchar(500)')     AS id_stoyanki
                ,data.value('(/subdivision/custom_elems/custom_elem[name="f_20u4"]/value)[1]', 'varchar(500)')          AS f_20u4
                ,data.value('(/subdivision/custom_elems/custom_elem[name="id_plowadki"]/value)[1]', 'varchar(500)')     AS id_plowadki
                ,data.value('(/subdivision/custom_elems/custom_elem[name="email"]/value)[1]', 'varchar(500)')           AS email
                ,data.value('(/subdivision/custom_elems/custom_elem[name="gps"]/value)[1]', 'varchar(500)')             AS gps
                ,data.value('(/subdivision/custom_elems/custom_elem[name="skidki"]/value)[1]', 'varchar(500)')          AS skidki
                ,data.value('(/subdivision/custom_elems/custom_elem[name="doc_obor"]/value)[1]', 'varchar(500)')        AS doc_obor
                ,data.value('(/subdivision/custom_elems/custom_elem[name="f_4xwo"]/value)[1]', 'varchar(500)')          AS f_4xwo
                ,data.value('(/subdivision/custom_elems/custom_elem[name="f_nliz"]/value)[1]', 'varchar(500)')          AS f_nliz
                ,data.value('(/subdivision/custom_elems/custom_elem[name="f_so4s"]/value)[1]', 'varchar(500)')          AS f_so4s
                ,data.value('(/subdivision/custom_elems/custom_elem[name="f_postpril1"]/value)[1]', 'varchar(500)')     AS f_postpril1
                ,data.value('(/subdivision/custom_elems/custom_elem[name="f_dilerskidki"]/value)[1]', 'varchar(500)')   AS f_dilerskidki
	    FROM    {0}
	    WHERE data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime')  > CAST('{1}' AS DATETIME2)
	        OR data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime')  > CAST('{1}' AS DATETIME2)
    ),
    sq2 AS(
        SELECT  id
                ,Table1.field.query('name').value('.', 'varchar(500)') 													AS plan_name
                ,Table1.field.value('(./value)[1]', 'varchar(500)') 													AS plan_value
	    FROM    {0}
	        CROSS APPLY subdivision.data.nodes('(/subdivision/custom_elems/custom_elem)') AS Table1(field)
	    WHERE data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime')  > CAST('{1}' AS DATETIME2)
	        OR data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime')  > CAST('{1}' AS DATETIME2)
    ),
    sq3 AS(
        SELECT *
        FROM sq2
        WHERE plan_name LIKE '%plan%'
    )
    SELECT sq1.*, sq3.plan_name, sq3.plan_value
    FROM sq1
    LEFT JOIN sq3
        ON sq1.id = sq3.id;