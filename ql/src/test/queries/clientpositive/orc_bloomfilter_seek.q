drop table if exists numtab_tmp_hive_table;

create table numtab_tmp_hive_table 
(c0 string, c1 string, c2 int, c3 float, c4 double, c5 string)
row format delimited fields terminated by ',';

load data local inpath '../../data/files/numtab_100k.csv' overwrite into table numtab_tmp_hive_table;


drop table if exists numtab_hive_orc_table;

create table numtab_hive_orc_table 
(c0 string, c1 string, c2 int, c3 float, c4 double, c5 string)
stored as orc
TBLPROPERTIES ("orc.compress"="NONE", 
               "orc.row.index.stride"="1000",
               "orc.stripe.size"="524288",
               "orc.bloom.filter.columns"="c2");

insert overwrite table numtab_hive_orc_table 
select * from numtab_tmp_hive_table sort by c2;



set hive.optimize.index.filter=true;

select * from numtab_hive_orc_table where c2=100 limit 5;

