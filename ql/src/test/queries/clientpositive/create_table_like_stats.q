--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t;

drop table a_n13;

create table a_n13 like src;

desc formatted a_n13;

drop table a_n13;

create table a_n13 like src location '${system:test.tmp.dir}/t';

desc formatted a_n13;

drop table a_n13;

create table a_n13 (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

desc formatted a_n13;

drop table a_n13;

create table a_n13 like srcpart;

desc formatted a_n13;
 
