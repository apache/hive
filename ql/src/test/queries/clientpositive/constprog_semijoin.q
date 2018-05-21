set hive.optimize.constant.propagation=true;
set hive.explain.user=true;

-- SORT_QUERY_RESULTS

create table table1_n10 (id int, val string, val1 string, dimid int);
insert into table1_n10 (id, val, val1, dimid) values (1, 't1val01', 'val101', 100), (2, 't1val02', 'val102', 200), (3, 't1val03', 'val103', 103), (3, 't1val01', 'val104', 100), (2, 't1val05', 'val105', 200), (3, 't1val01', 'val106', 103), (1, 't1val07', 'val107', 200), (2, 't1val01', 'val108', 200), (3, 't1val09', 'val109', 103), (4,'t1val01', 'val110', 200);

create table table2_n6 (id int, val2 string);
insert into table2_n6 (id, val2) values (1, 't2val201'), (2, 't2val202'), (3, 't2val203');

create table table3_n0 (id int);
insert into table3_n0 (id) values (100), (100), (101), (102), (103);

explain select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id where table1_n10.val = 't1val01';
select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id where table1_n10.val = 't1val01';

explain select table1_n10.id, table1_n10.val, table2_n6.val2 from table1_n10 inner join table2_n6 on table1_n10.val = 't1val01' and table1_n10.id = table2_n6.id left semi join table3_n0 on table1_n10.dimid = table3_n0.id;
select table1_n10.id, table1_n10.val, table2_n6.val2 from table1_n10 inner join table2_n6 on table1_n10.val = 't1val01' and table1_n10.id = table2_n6.id left semi join table3_n0 on table1_n10.dimid = table3_n0.id;

explain select table1_n10.id, table1_n10.val, table2_n6.val2 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id inner join table2_n6 on table1_n10.val = 't1val01' and table1_n10.id = table2_n6.id;
select table1_n10.id, table1_n10.val, table2_n6.val2 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id inner join table2_n6 on table1_n10.val = 't1val01' and table1_n10.id = table2_n6.id;

explain select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid <> 100;
select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid <> 100;

explain select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid  IN (100,200);
select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid  IN (100,200);

explain select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid  = 200;
select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid  = 200;

explain select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid  = 100;
select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100 where table1_n10.dimid  = 100;

explain select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100;
select table1_n10.id, table1_n10.val, table1_n10.val1 from table1_n10 left semi join table3_n0 on table1_n10.dimid = table3_n0.id and table3_n0.id = 100;
