--! qt:dataset:src

create external table external1 (key string, value string) stored as textfile;
load data local inpath '../../data/files/kv1.txt' into table external1;
select count(*) from external1;
truncate table external1 force;
select count(*) from external1;


-- Partitioned table
create external table external2 (key string, value string) partitioned by (p1 string) stored as textfile;
load data local inpath '../../data/files/kv1.txt' into table external2 partition (p1='abc');
select count(*) from external2;
truncate table external2 partition (p1='abc') force;
select count(*) from external2;

