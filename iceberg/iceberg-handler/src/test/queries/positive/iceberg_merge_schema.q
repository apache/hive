-- SORT_QUERY_RESULTS

-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/

set hive.optimize.shared.work.merge.ts.schema=true;
set hive.vectorized.execution.enabled=true;

CREATE EXTERNAL TABLE calls (
  s_key bigint, 
  year int
) PARTITIONED BY SPEC (year)  
STORED BY Iceberg STORED AS parquet 
TBLPROPERTIES ('format-version'='2');
  
INSERT INTO calls (s_key, year) VALUES (1090969, 2022);


CREATE EXTERNAL TABLE display (                   
  skey bigint,                                   
  hierarchy_number string,                       
  hierarchy_name string,                         
  language_id int,                               
  hierarchy_display string,                      
  orderby string
)                                
STORED BY Iceberg STORED AS parquet 
TBLPROPERTIES ('format-version'='2'); 

INSERT INTO display (skey, language_id, hierarchy_display) VALUES 
  (1090969, 3, 'f9e59bae9b131de1d8f02d887ee91e20-mergeupdated1-updated1'),
  (1090969, 3, 'f9e59bae9b131de1d8f02d887ee91e20-mergeupdated1-updated1-insertnew1');


explain vectorization only detail MERGE INTO display USING (
  SELECT distinct display_skey, display, display as orig_display 
  FROM (
    SELECT D.skey display_skey, D.hierarchy_display display
    FROM ( 
      SELECT s_key FROM calls WHERE s_key =  1090969
    ) R 
    INNER JOIN display D 
      ON R.s_key = D.skey AND D.language_id = 3 
    GROUP BY D.skey, 
      D.hierarchy_display
  ) sub1 

  UNION ALL 
  
  SELECT distinct display_skey, null as display, display as orig_display 
  FROM (
    SELECT D.skey display_skey, D.hierarchy_display display
    FROM ( 
      SELECT s_key FROM calls WHERE s_key =  1090969
    ) R 
    INNER JOIN display D 
      ON R.s_key = D.skey AND D.language_id = 3 
    GROUP BY D.skey,
      D.hierarchy_display
  ) sub2
) sub 
ON display.skey = sub.display_skey 
    and display.hierarchy_display = sub.display 

WHEN MATCHED THEN 
  UPDATE SET hierarchy_display = concat(sub.display, '-mergeupdated1') 
WHEN NOT MATCHED THEN 
  INSERT (skey, language_id, hierarchy_display) values (sub.display_skey, 3, concat(sub.orig_display, '-mergenew1'));
  
  
MERGE INTO display USING (
  SELECT distinct display_skey, display, display as orig_display 
  FROM (
    SELECT D.skey display_skey, D.hierarchy_display display
    FROM ( 
      SELECT s_key FROM calls WHERE s_key =  1090969
    ) R 
    INNER JOIN display D 
      ON R.s_key = D.skey AND D.language_id = 3 
    GROUP BY D.skey, 
      D.hierarchy_display
  ) sub1 

  UNION ALL 
  
  SELECT distinct display_skey, null as display, display as orig_display 
  FROM (
    SELECT D.skey display_skey, D.hierarchy_display display
    FROM ( 
      SELECT s_key FROM calls WHERE s_key =  1090969
    ) R 
    INNER JOIN display D 
      ON R.s_key = D.skey AND D.language_id = 3 
    GROUP BY D.skey,
      D.hierarchy_display
  ) sub2
) sub 
ON display.skey = sub.display_skey 
    and display.hierarchy_display = sub.display 

WHEN MATCHED THEN 
  UPDATE SET hierarchy_display = concat(sub.display, '-mergeupdated1') 
WHEN NOT MATCHED THEN 
  INSERT (skey, language_id, hierarchy_display) values (sub.display_skey, 3, concat(sub.orig_display, '-mergenew1'));


SELECT * FROM display;
    
-- clean up    
DROP TABLE calls;
DROP TABLE display;