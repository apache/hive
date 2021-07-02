
drop table if exists t;
drop table if exists t2;
drop table if exists t3;
create table t (a integer);
create table t2 (a integer);
create table t3 (a integer);
alter table t set tblproperties ('bucketing_version'='1');
alter table t2 set tblproperties ('bucketing_version'='2');
alter table t3 set tblproperties ('bucketing_version'='2');

-- this should be allowed: we may write to t,t2 locations from the same operator group since they both are non-bucketed tables
explain
from t3
insert into t select a
insert into t2 select a;

