--! qt:dataset:src
set hive.mapred.mode=nonstrict;
SET hive.map.aggr=true;
SET hive.multigroupby.singlereducer=false;
SET hive.groupby.skewindata=false;
SET mapred.reduce.tasks=31;
SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

-- JOIN TEST

EXPLAIN
FROM
(SELECT src.* FROM src sort by key) X
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (X.key = Y.key)
JOIN
(SELECT src.* FROM src sort by value) Z
ON (X.key = Z.key)
SELECT sum(hash(Y.key,Y.value)) GROUP BY Y.key;


CREATE TABLE dest1_n90(key INT, value STRING);
CREATE TABLE dest2_n24(key INT, value STRING);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n90 SELECT src.key, sum(SUBSTR(src.value,5)) GROUP BY src.key
INSERT OVERWRITE TABLE dest2_n24 SELECT src.key, sum(SUBSTR(src.value,5)) GROUP BY src.key;

SELECT dest1_n90.* FROM dest1_n90;
SELECT dest2_n24.* FROM dest2_n24;

DROP TABLE dest1_n90;
DROP TABLE dest2_n24;


-- UNION TEST

CREATE TABLE tmptable_n8(key STRING, value INT);

EXPLAIN
INSERT OVERWRITE TABLE tmptable_n8
  SELECT unionsrc.key, unionsrc.value FROM (SELECT 'tst1' AS key, count(1) AS value FROM src s1
                                        UNION  ALL
                                            SELECT 'tst2' AS key, count(1) AS value FROM src s2
                                        UNION ALL
                                            SELECT 'tst3' AS key, count(1) AS value FROM src s3) unionsrc;
SELECT * FROM tmptable_n8 x SORT BY x.key;

DROP TABLE tmtable;


EXPLAIN
SELECT unionsrc1.key, unionsrc1.value, unionsrc2.key, unionsrc2.value
FROM (SELECT 'tst1' AS key, cast(count(1) AS string) AS value FROM src s1
                         UNION  ALL
      SELECT s2.key AS key, s2.value AS value FROM src s2 WHERE s2.key < 10) unionsrc1
JOIN
     (SELECT 'tst1' AS key, cast(count(1) AS string) AS value FROM src s3
                         UNION  ALL
      SELECT s4.key AS key, s4.value AS value FROM src s4 WHERE s4.key < 10) unionsrc2
ON (unionsrc1.key = unionsrc2.key);


-- CWE TEST

CREATE TABLE inv(w_warehouse_name STRING , w_warehouse_sk INT , stdev INT , d_moy INT , mean INT , cov INT , inv_quantity_on_hand INT);
CREATE TABLE inventory(inv_date_sk INT , inv_item_sk INT ,inv_quantity_on_hand INT ,inv_warehouse_sk INT);
CREATE TABLE item(i_item_sk INT);
CREATE TABLE warehouse(w_warehouse_sk INT , w_warehouse_name STRING);
CREATE TABLE date_dim(d_date_sk INT , d_year INT , d_moy INT);

EXPLAIN
WITH inv AS
(SELECT w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, CASE mean WHEN 0 THEN null ELSE stdev/mean END cov
FROM(SELECT w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,STDDEV_SAMP(inv_quantity_on_hand) stdev,AVG(inv_quantity_on_hand) mean
      FROM inventory
          ,item
          ,warehouse
          ,date_dim
      WHERE inv_item_sk = i_item_sk
        AND inv_warehouse_sk = w_warehouse_sk
        AND inv_date_sk = d_date_sk
        AND d_year =1999
      GROUP BY w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 WHERE CASE mean WHEN 0 THEN 0 ELSE stdev/mean END > 1)
SELECT inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
FROM inv inv1,inv inv2
WHERE inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  AND inv1.d_moy=3
  AND inv2.d_moy=3+1
ORDER BY inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;

EXPLAIN
WITH test AS
(SELECT inv_date_sk , inv_item_sk ,inv_quantity_on_hand FROM inventory
  UNION ALL
 SELECT inv_date_sk , inv_item_sk ,inv_quantity_on_hand FROM inventory)
SELECT inv_date_sk , inv_item_sk ,inv_quantity_on_hand FROM test SORT BY inv_quantity_on_hand;

DROP TABLE inv;
DROP TABLE inventory;
DROP TABLE item;
DROP TABLE warehouse;
DROP TABLE date_dim;
