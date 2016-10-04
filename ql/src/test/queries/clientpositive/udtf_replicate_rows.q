set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;

DESCRIBE FUNCTION replicate_rows;
DESCRIBE FUNCTION EXTENDED replicate_rows;

create table t (x bigint, y string, z int);

insert into table t values (3,'2',0),(2,'3',1),(0,'2',2),(-1,'k',3);

SELECT replicate_rows(x,y) FROM t;

SELECT replicate_rows(x,y,y) FROM t;

SELECT replicate_rows(x,y,y,y,z) FROM t;

select y,x from (SELECT replicate_rows(x,y) as (x,y) FROM t)subq;

select z,y,x from(SELECT replicate_rows(x,y,y) as (z,y,x) FROM t)subq;

SELECT replicate_rows(x,concat(y,'...'),y) FROM t;


