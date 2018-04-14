--! qt:dataset:src
set hive.fetch.task.conversion=more;

create table array_table (`array` array<string>, index int );
insert into table array_table select array('first', 'second', 'third'), key%3 from src tablesample (4 rows);

explain
select index, `array`[index] from array_table;
select index, `array`[index] from array_table;

create table map_table (data map<string,string>, key int );
insert into table map_table select map('1','one','2','two','3','three'), cast((key%3+1) as int) from src tablesample (4 rows);

explain
select key, data[key] from map_table;
select key, data[key] from map_table;
