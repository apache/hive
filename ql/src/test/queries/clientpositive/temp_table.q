--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
EXPLAIN CREATE TEMPORARY TABLE foo AS SELECT * FROM src WHERE key % 2 = 0;
CREATE TEMPORARY TABLE foo AS SELECT * FROM src WHERE key % 2 = 0;

EXPLAIN CREATE TEMPORARY TABLE bar AS SELECT * FROM src WHERE key % 2 = 1;
CREATE TEMPORARY TABLE bar AS SELECT * FROM src WHERE key % 2 = 1;

DESCRIBE foo;
DESCRIBE FORMATTED bar;

explain select * from foo order by key limit 10;
select * from foo order by key limit 10;

explain select * from (select * from foo union all select * from bar) u order by key limit 10;
select * from (select * from foo union all select * from bar) u order by key limit 10;

CREATE TEMPORARY TABLE baz LIKE foo;

INSERT OVERWRITE TABLE baz SELECT * from foo;

CREATE TEMPORARY TABLE bay (key string, value string) STORED AS orc;
select * from bay;

INSERT OVERWRITE TABLE bay SELECT * FROM src ORDER BY key;

select * from bay order by key limit 10;

CREATE DATABASE two;

USE two;

SHOW TABLES;

CREATE TEMPORARY TABLE foo AS SELECT * FROM default.foo;

SHOW TABLES;

use default;

DROP DATABASE two CASCADE;

DROP TABLE bay;

create table s_n4 as select * from src limit 10;

select count(*) from s_n4;

create temporary table s_n4 as select * from s_n4 limit 2;

select count(*) from s_n4;

with s_n4 as ( select * from src limit 1)
select count(*) from s_n4;

with src as ( select * from s_n4)
select count(*) from src;

drop table s_n4;

select count(*) from s_n4;

with s_n4 as ( select * from src limit 1)
select count(*) from s_n4;

with src as ( select * from s_n4)
select count(*) from src;

drop table s_n4;
