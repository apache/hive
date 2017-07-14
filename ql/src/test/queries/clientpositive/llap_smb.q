set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;

set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE orc_a;
DROP TABLE orc_b;

CREATE TABLE orc_a (id bigint, cdouble double) partitioned by (y int, q smallint)
  CLUSTERED BY (id) SORTED BY (id) INTO 2 BUCKETS stored as orc;
CREATE TABLE orc_b (id bigint, cfloat float)
  CLUSTERED BY (id) SORTED BY (id) INTO 2 BUCKETS stored as orc;

insert into table orc_a partition (y=2000, q)
select cbigint, cdouble, csmallint % 10 from alltypesorc
  where cbigint is not null and csmallint > 0 order by cbigint asc;
insert into table orc_a partition (y=2001, q)
select cbigint, cdouble, csmallint % 10 from alltypesorc
  where cbigint is not null and csmallint > 0 order by cbigint asc;

insert into table orc_b 
select cbigint, cfloat from alltypesorc
  where cbigint is not null and csmallint > 0 order by cbigint asc limit 200;

set hive.cbo.enable=false;

select y,q,count(*) from orc_a a join orc_b b on a.id=b.id group by y,q;



