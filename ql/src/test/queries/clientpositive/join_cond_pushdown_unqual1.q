--! qt:dataset:part
set hive.mapred.mode=nonstrict;
create table part2_n0( 
    p2_partkey INT,
    p2_name STRING,
    p2_mfgr STRING,
    p2_brand STRING,
    p2_type STRING,
    p2_size INT,
    p2_container STRING,
    p2_retailprice DOUBLE,
    p2_comment STRING
);

create table part3_n0( 
    p3_partkey INT,
    p3_name STRING,
    p3_mfgr STRING,
    p3_brand STRING,
    p3_type STRING,
    p3_size INT,
    p3_container STRING,
    p3_retailprice DOUBLE,
    p3_comment STRING
);

explain select *
from part p1 join part2_n0 p2 join part3_n0 p3 on p1.p_name = p2_name and p2_name = p3_name;

explain select *
from part p1 join part2_n0 p2 join part3_n0 p3 on p2_name = p1.p_name and p3_name = p2_name;

explain select *
from part p1 join part2_n0 p2 join part3_n0 p3 on p2_partkey + p_partkey = p1.p_partkey and p3_name = p2_name;

explain select *
from part p1 join part2_n0 p2 join part3_n0 p3 on p2_partkey = 1 and p3_name = p2_name;
