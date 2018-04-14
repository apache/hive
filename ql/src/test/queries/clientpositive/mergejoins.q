--! qt:dataset:src
set hive.mapred.mode=nonstrict;
create table a_n0 (val1 int, val2 int);
create table b_n0 (val1 int, val2 int);
create table c_n0 (val1 int, val2 int);
create table d_n0 (val1 int, val2 int);
create table e_n0 (val1 int, val2 int);

explain select * from a_n0 join b_n0 on a_n0.val1=b_n0.val1 join c_n0 on a_n0.val1=c_n0.val1 join d_n0 on a_n0.val1=d_n0.val1 join e_n0 on a_n0.val2=e_n0.val2;

--HIVE-3070 filter on outer join condition removed while merging join tree
explain select * from src a_n0 join src b_n0 on a_n0.key=b_n0.key left outer join src c_n0 on b_n0.key=c_n0.key and b_n0.key<10;
