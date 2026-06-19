set hive.mapred.mode=nonstrict;

create temporary table table1(id int, val int, val1 int, dimid int);
create temporary table table3(id int, val int, val1 int);

explain
select table1.id, table1.val, table1.val1
from table1 inner join table3
on table1.dimid = table3.id and table3.id = 1 where table1.dimid <> 1;

