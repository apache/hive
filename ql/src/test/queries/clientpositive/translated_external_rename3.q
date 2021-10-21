set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=force;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

create external table t (a integer);
insert into t values(1);
alter table t rename to t2;

create table t (a integer);
insert into t values(2);

select assert_true(count(1) = 2) from t;
select assert_true(count(1) = 2) from t2;

select * from t;
select * from t2;

desc formatted t;
desc formatted t2;


