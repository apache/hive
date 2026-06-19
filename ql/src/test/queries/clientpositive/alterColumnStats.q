set hive.mapred.mode=nonstrict;

drop table p_n0;

CREATE TABLE p_n0(insert_num int, c1 tinyint, c2 smallint);

desc formatted p_n0;

insert into p_n0 values (1,22,333);

desc formatted p_n0;

alter table p_n0 replace columns (insert_num int, c1 STRING, c2 STRING);

desc formatted p_n0;

desc formatted p_n0 c1;

desc formatted p_n0 c2;


