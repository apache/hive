SELECT * FROM a where 1=1 and not exists (select * from b)--abc;

SELECT * 
 FROM a 
 where not exists 
  (
    select * from b
  );
  
SELECT 
         *
         FROM
         tab
         WHERE FILE_DATE > (
                           SELECT 
                           MAX(FILE_DATE) AS MX_C_FILE_DT
                           FROM tab
                           WHERE FLAG = 'C' 
                           AND IND = 'C'
                           AND FILE_DATE < 
                                          ( SELECT 
                                            CAST( LOAD_START AS DATE) 
                                            FROM 
                                            tab  
                                            WHERE
                				              SOURCE_ID = 451 AND
                				              BATCH = 'R'
                		                   )
		                  );
                          
SELECT 
*
FROM
 DLTA_POC
  LEFT OUTER JOIN TEST3_DB.TET ORG
   ON DLTA_POC.YS_NO = ORG.EM_CODE_A
   AND DLTA_POC.AREA_NO = ORG.AREA_CODE_2
   AND DLTA_POC.GNT_POC = ORG.GEN_CD

  LEFT OUTER JOIN TEST.LOCATION LOC
   ON DLTA_POC.SE_KEY_POC = LOC.LOC_ID
   AND LOC.LOCATION_END_DT = DATE '9999-12-31' ;

SELECT *
  FROM a
  WHERE NOT (1 = 2)