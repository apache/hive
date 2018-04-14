--! qt:dataset:part
set hive.mapred.mode=nonstrict;
create table part2( 
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

create table part3( 
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
from part p1 join part2 p2 join part3 p3 on p1.p_name = p2_name join part p4 on p2_name = p3_name and p1.p_name = p4.p_name;

explain select *
from part p1 join part2 p2 join part3 p3 on p2_name = p1.p_name join part p4 on p2_name = p3_name and p1.p_partkey = p4.p_partkey 
            and p1.p_partkey = p2_partkey;
