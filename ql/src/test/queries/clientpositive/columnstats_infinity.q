set hive.stats.column.autogather=false;

CREATE TABLE schema_evolution_data(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data.txt' overwrite into table schema_evolution_data;

CREATE TABLE table_change_numeric_group_string_group_floating_string_group(insert_num int,
              c1 decimal(38,18), c2 float, c3 double,
              c4 decimal(38,18), c5 float, c6 double, c7 decimal(38,18), c8 float, c9 double,
              c10 decimal(38,18), c11 float, c12 double, c13 decimal(38,18), c14 float, c15 double,
              b STRING);

insert into table table_change_numeric_group_string_group_floating_string_group SELECT insert_num,
              decimal1, float1, double1,
              decimal1, float1, double1, decimal1, float1, double1,
              decimal1, float1, double1, decimal1, float1, double1,
             'original' FROM schema_evolution_data;

desc formatted table_change_numeric_group_string_group_floating_string_group;

analyze table table_change_numeric_group_string_group_floating_string_group compute statistics for columns;

desc formatted table_change_numeric_group_string_group_floating_string_group;

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,b from table_change_numeric_group_string_group_floating_string_group;

set hive.stats.column.autogather=true;

drop table table_change_numeric_group_string_group_floating_string_group;

CREATE TABLE table_change_numeric_group_string_group_floating_string_group(insert_num int,
              c1 decimal(38,18), c2 float, c3 double,
              c4 decimal(38,18), c5 float, c6 double, c7 decimal(38,18), c8 float, c9 double,
              c10 decimal(38,18), c11 float, c12 double, c13 decimal(38,18), c14 float, c15 double,
              b STRING);

insert into table table_change_numeric_group_string_group_floating_string_group SELECT insert_num,
              decimal1, float1, double1,
              decimal1, float1, double1, decimal1, float1, double1,
              decimal1, float1, double1, decimal1, float1, double1,
             'original' FROM schema_evolution_data;

desc formatted table_change_numeric_group_string_group_floating_string_group;

