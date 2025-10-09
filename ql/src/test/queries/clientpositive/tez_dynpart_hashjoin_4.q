CREATE TABLE table_a (start_date date, product_id int);

ALTER TABLE table_a UPDATE STATISTICS SET('numRows'='200000000','rawDataSize'='0' );
ALTER TABLE table_a UPDATE STATISTICS FOR COLUMN product_id SET('lowValue'='1000000','highValue'='100000000','numNulls'='0','numDVs'='300000' );
ALTER TABLE table_a UPDATE STATISTICS FOR COLUMN start_date SET('lowValue'='10000','highValue'='20000','numNulls'='0','numDVs'='2500' );

CREATE TABLE table_b (start_date date, product_id int, product_sk string);

ALTER TABLE table_b UPDATE STATISTICS SET('numRows'='100000000','rawDataSize'='0' );
ALTER TABLE table_b UPDATE STATISTICS FOR COLUMN product_id SET('lowValue'='1000000','highValue'='100000000','numNulls'='0','numDVs'='300000' );
ALTER TABLE table_b UPDATE STATISTICS FOR COLUMN start_date SET('lowValue'='10000','highValue'='20000','numNulls'='0','numDVs'='500' );
ALTER TABLE table_b UPDATE STATISTICS FOR COLUMN product_sk SET ('numDVs'='300000','numNulls'='0','avgColLen'='10','maxColLen'='10');

set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=180000000;

EXPLAIN
SELECT TC.CONST_DATE, TB.PRODUCT_SK
FROM TABLE_A TA
INNER JOIN (SELECT TO_DATE(FROM_UNIXTIME(1701088643)) AS CONST_DATE) TC
    ON TA.START_DATE = TC.CONST_DATE
INNER JOIN TABLE_B TB
    ON TB.START_DATE = TC.CONST_DATE AND TA.PRODUCT_ID = TB.PRODUCT_ID;
