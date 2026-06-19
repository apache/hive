--! qt:dataset:src
-- Test partition column type is considered as the type given in table def
-- and not as 'string'
CREATE TEMPORARY TABLE datePartTbl_temp(col1 string) PARTITIONED BY (date_prt date);

-- Add test partitions and some sample data
INSERT OVERWRITE TABLE datePartTbl_temp PARTITION(date_prt='2014-08-09')
  SELECT 'col1-2014-08-09' FROM src LIMIT 1;

INSERT OVERWRITE TABLE datePartTbl_temp PARTITION(date_prt='2014-08-10')
  SELECT 'col1-2014-08-10' FROM src LIMIT 1;

-- Query where 'date_prt' value is restricted to given values in IN operator.
SELECT * FROM datePartTbl_temp WHERE date_prt IN (CAST('2014-08-09' AS DATE), CAST('2014-08-08' AS DATE));

DROP TABLE datePartTbl_temp;
