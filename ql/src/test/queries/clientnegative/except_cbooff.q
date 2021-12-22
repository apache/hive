set hive.cbo.enable=false;
create table except_nocbo(id int);
insert into table except_nocbo values(1);
select id from except_nocbo except select id from except_nocbo;

