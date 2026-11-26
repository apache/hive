-- HIVE-30037: in an INSERT with a column list, positions count the select list, which has one column
create table pacdi_s (x int, y int);
create table pacdi_t (x int, y int);
insert into pacdi_t (x) select y from pacdi_s tablesample (5 rows) order by 2 limit 1;
