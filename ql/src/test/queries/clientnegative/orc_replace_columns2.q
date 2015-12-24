create table src_orc (key tinyint, val string) stored as orc;
alter table src_orc replace columns (k smallint, val string);
