--! qt:dataset:alltypesorc

-- MASK_STATS

set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=true;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;


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



SET hive.llap.io.enabled=true;
set hive.enforce.sortmergebucketmapjoin=false;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=10;

explain
select y,q,count(*) from orc_a a join orc_b b on a.id=b.id group by y,q;

-- The results are currently incorrect. See HIVE-16985/HIVE-16965

select y,q,count(*) from orc_a a join orc_b b on a.id=b.id group by y,q;

DROP TABLE orc_a;
DROP TABLE orc_b;
