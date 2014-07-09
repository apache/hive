
-- Test temp tables with upper/lower case names
create temporary table Default.Temp_Table_Names (C1 string, c2 string);

show tables 'Temp_Table*';
show tables in default 'temp_table_names';
show tables in DEFAULT 'TEMP_TABLE_NAMES';

select c1 from default.temp_table_names;
select C1 from DEFAULT.TEMP_TABLE_NAMES;

drop table Default.TEMP_TABLE_names;
show tables 'temp_table_names';
