set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=force;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

create external table t (a integer);
insert into t values(1);
alter table t rename to t2;

-- this TRANSLATED table will have its location shared with the pre-existing t2 table
create table t (a integer);
insert into t values(2);

-- the rows from bot T and T2 can be seen from both tables
select assert_true(count(1) = 2) from t;
select assert_true(count(1) = 2) from t2;

select * from t;
select * from t2;

-- the location of both T and T2 is the same
desc formatted t;
desc formatted t2;


