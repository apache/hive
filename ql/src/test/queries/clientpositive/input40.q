drop table tmp_insert_test;
drop table tmp_insert_test_p;

create table tmp_insert_test (key string, value string) stored as textfile;
load data local inpath '../data/files/kv1.txt' into table tmp_insert_test;
select * from tmp_insert_test
SORT BY key ASC;

create table tmp_insert_test_p (key string, value string) partitioned by (ds string) stored as textfile;

load data local inpath '../data/files/kv1.txt' into table tmp_insert_test_p partition (ds = '2009-08-01');
select * from tmp_insert_test_p where ds= '2009-08-01'
SORT BY key ASC;

load data local inpath '../data/files/kv2.txt' into table tmp_insert_test_p partition (ds = '2009-08-01');
select * from tmp_insert_test_p where ds= '2009-08-01'
SORT BY key ASC;

drop table tmp_insert_test;
drop table tmp_insert_test_p;
