--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

DROP TABLE insert_into1_n1;
DROP TABLE ctas_table;
DROP TABLE ctas_part;

CREATE TABLE insert_into1_n1 (key int, value string);

-- TODO: Remove this line after fixing HIVE-24532
set hive.vectorized.execution.enabled=false;
INSERT OVERWRITE TABLE insert_into1_n1 SELECT * from src ORDER BY key LIMIT 10;

-- TODO: Remove this line after fixing HIVE-24532
set hive.vectorized.execution.enabled=true;

select * from insert_into1_n1 order by key;

INSERT INTO TABLE insert_into1_n1 SELECT * from src ORDER BY key DESC LIMIT 10;

select * from insert_into1_n1 order by key;

CREATE TABLE ctas_table AS SELECT key, count(value) AS foo FROM src GROUP BY key ORDER BY key LIMIT 10;

describe extended ctas_table;

select * from ctas_table order by key;


set hive.exec.dynamic.partition=true;

create table ctas_part (key int, value string) partitioned by (modkey bigint);

-- TODO: Remove this line after fixing HIVE-24532
set hive.vectorized.execution.enabled=false;

insert overwrite table ctas_part partition (modkey) 
select key, value, ceil(key / 100) from src where key is not null order by key limit 10;

-- TODO: Remove this line after fixing HIVE-24532
set hive.vectorized.execution.enabled=true;

select * from ctas_part order by key;



DROP TABLE insert_into1_n1;
DROP TABLE ctas_table;
DROP TABLE ctas_part;
