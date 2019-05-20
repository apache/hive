set hive.mapred.mode=nonstrict;
set hive.stats.column.autogather=true;

drop table p_n1;

CREATE TABLE p_n1(insert_num int, c1 tinyint, c2 smallint);

desc formatted p_n1;

insert into p_n1 values (1,22,333);

desc formatted p_n1;

alter table p_n1 replace columns (insert_num int, c1 STRING, c2 STRING);

desc formatted p_n1;

desc formatted p_n1 insert_num;
desc formatted p_n1 c1;

insert into p_n1 values (2,11,111);

desc formatted p_n1;

desc formatted p_n1 insert_num;
desc formatted p_n1 c1;

set hive.stats.column.autogather=false;

drop table p_n1;

CREATE TABLE p_n1(insert_num int, c1 tinyint, c2 smallint);

desc formatted p_n1;

insert into p_n1 values (1,22,333);

desc formatted p_n1;

alter table p_n1 replace columns (insert_num int, c1 STRING, c2 STRING);

desc formatted p_n1;

desc formatted p_n1 insert_num;
desc formatted p_n1 c1;

insert into p_n1 values (2,11,111);

desc formatted p_n1;

desc formatted p_n1 insert_num;
desc formatted p_n1 c1;
