set hive.optimize.constant.propagation=true;
set hive.explain.user=true;

create table table1 (id int, val string, val1 string, dimid int);
insert into table1 (id, val, val1, dimid) values (1, 't1val01', 'val101', 100), (2, 't1val02', 'val102', 200), (3, 't1val03', 'val103', 103), (3, 't1val01', 'val104', 100), (2, 't1val05', 'val105', 200), (3, 't1val01', 'val106', 103), (1, 't1val07', 'val107', 200), (2, 't1val01', 'val108', 200), (3, 't1val09', 'val109', 103), (4,'t1val01', 'val110', 200);

create table table2 (id int, val2 string);
insert into table2 (id, val2) values (1, 't2val201'), (2, 't2val202'), (3, 't2val203');

create table table3 (id int);
insert into table3 (id) values (100), (100), (101), (102), (103);

explain select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id where table1.val = 't1val01';
select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id where table1.val = 't1val01';

explain select table1.id, table1.val, table2.val2 from table1 inner join table2 on table1.val = 't1val01' and table1.id = table2.id left semi join table3 on table1.dimid = table3.id;
select table1.id, table1.val, table2.val2 from table1 inner join table2 on table1.val = 't1val01' and table1.id = table2.id left semi join table3 on table1.dimid = table3.id;

explain select table1.id, table1.val, table2.val2 from table1 left semi join table3 on table1.dimid = table3.id inner join table2 on table1.val = 't1val01' and table1.id = table2.id;
select table1.id, table1.val, table2.val2 from table1 left semi join table3 on table1.dimid = table3.id inner join table2 on table1.val = 't1val01' and table1.id = table2.id;

explain select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid <> 100;
select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid <> 100;

explain select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid  IN (100,200);
select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid  IN (100,200);

explain select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid  = 200;
select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid  = 200;

explain select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid  = 100;
select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100 where table1.dimid  = 100;

explain select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100;
select table1.id, table1.val, table1.val1 from table1 left semi join table3 on table1.dimid = table3.id and table3.id = 100;
