--! qt:dataset:part
EXPLAIN CBO
SELECT x.p_partkey
FROM part x
WHERE EXISTS (SELECT 1
              FROM part y
              WHERE x.p_name = y.p_name
                AND y.p_brand IN (SELECT 'Brand#32'));
SELECT x.p_partkey
FROM part x
WHERE EXISTS (SELECT 1
              FROM part y
              WHERE x.p_name = y.p_name
                AND y.p_brand IN (SELECT 'Brand#32'));

EXPLAIN CBO
SELECT x.p_partkey, x.p_mfgr
FROM part x
WHERE x.p_mfgr IN ('Manufacturer#1', 'Manufacturer#2', 'Manufacturer#3')
AND EXISTS (SELECT 1
              FROM part y
              WHERE x.p_name = y.p_name
                AND y.p_brand IN (SELECT 'Brand#32'));
SELECT x.p_partkey, x.p_mfgr
FROM part x
WHERE x.p_mfgr IN ('Manufacturer#1', 'Manufacturer#2', 'Manufacturer#3')
AND EXISTS (SELECT 1
              FROM part y
              WHERE x.p_name = y.p_name
                AND y.p_brand IN (SELECT 'Brand#32'));
