create table concat_mm (id int) stored as orc tblproperties('hivecommit'='true');

insert into table concat_mm select key from src limit 10;

alter table concat_mm concatenate;
