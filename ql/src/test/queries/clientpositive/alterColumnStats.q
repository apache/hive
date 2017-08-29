set hive.mapred.mode=nonstrict;

drop table p;

CREATE TABLE p(insert_num int, c1 tinyint, c2 smallint);

desc formatted p;

insert into p values (1,22,333);

desc formatted p;

alter table p replace columns (insert_num int, c1 STRING, c2 STRING);

desc formatted p;

desc formatted p c1;

desc formatted p c2;


