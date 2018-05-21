--! qt:dataset:part
set hive.mapred.mode=nonstrict;
set hive.typecheck.on.insert = true;

-- begin part(string, string) pass(string, int)
CREATE TABLE tab1_n3 (id1 int,id2 string) PARTITIONED BY(month string,day string) stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' overwrite into table tab1_n3 PARTITION(month='June', day=2);

select * from tab1_n3;
drop table tab1_n3;

-- begin part(string, int) pass(string, string)
CREATE TABLE tab1_n3 (id1 int,id2 string) PARTITIONED BY(month string,day int) stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' overwrite into table tab1_n3 PARTITION(month='June', day='2');

select * from tab1_n3;
drop table tab1_n3;

-- begin part(string, date) pass(string, date)
create table tab1_n3 (id1 int, id2 string) PARTITIONED BY(month string,day date) stored as textfile;
alter table tab1_n3 add partition (month='June', day='2008-01-01');
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' overwrite into table tab1_n3 PARTITION(month='June', day='2008-01-01');

select id1, id2, day from tab1_n3 where day='2008-01-01';
drop table tab1_n3;

