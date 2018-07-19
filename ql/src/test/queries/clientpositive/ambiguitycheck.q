--! qt:dataset:src
-- check cluster/distribute/partitionBy
SELECT * FROM SRC x where x.key = 20 CLUSTER BY (key,value) ;

SELECT * FROM SRC x where x.key = 20 CLUSTER BY ((key),value) ;

SELECT * FROM SRC x where x.key = 20 CLUSTER BY (key,(value)) ;

SELECT * FROM SRC x where x.key = 20 CLUSTER BY ((key),(value)) ;

SELECT * FROM SRC x where x.key = 20 CLUSTER BY ((key),(((value))));

-- HIVE-6950
SELECT tab1.key,
       tab1.value,
       SUM(1)
FROM src as tab1
GROUP BY tab1.key,
         tab1.value
GROUPING SETS ((tab1.key, tab1.value));

SELECT key,
       src.value,
       SUM(1)
FROM src
GROUP BY key,
         src.value
GROUPING SETS ((key, src.value));

explain extended select int(1.2) from src limit 1;
select int(1.2) from src limit 1;
select bigint(1.34) from src limit 1;
select binary('1') from src limit 1;
select boolean(1) from src limit 1;
select date('1') from src limit 2;
select double(1) from src limit 1;
select float(1) from src limit 1;
select smallint(0.9) from src limit 1;
select timestamp('1') from src limit 2;

explain extended desc default.src key;

desc default.src key;
