set hive.mapred.mode=nonstrict;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t;

drop table a;

create table a like src;

desc formatted a;

drop table a;

create table a like src location '${system:test.tmp.dir}/t';

desc formatted a;

drop table a;

create table a (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

desc formatted a;

drop table a;

create table a like srcpart;

desc formatted a;
 
