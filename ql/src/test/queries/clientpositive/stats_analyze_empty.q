set hive.stats.autogather=true;
set hive.explain.user=true;

drop table if exists testdeci2;

create table testdeci2(
id int,
amount decimal(10,3),
sales_tax decimal(10,3),
item string)
stored as orc location '/tmp/testdeci2'
TBLPROPERTIES ("transactional"="false")
;


analyze table testdeci2 compute statistics for columns;

insert into table testdeci2 values(1,12.123,12345.123,'desk1'),(2,123.123,1234.123,'desk2');
