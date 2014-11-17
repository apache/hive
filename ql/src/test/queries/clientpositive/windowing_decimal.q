create table part_dec(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice decimal(10,3),
    p_comment STRING
);

insert overwrite table part_dec select * from part;

select p_mfgr, p_retailprice, 
first_value(p_retailprice) over(partition by p_mfgr order by p_retailprice) ,
sum(p_retailprice) over(partition by p_mfgr order by p_retailprice)
from part_dec;

select p_mfgr, p_retailprice, 
first_value(p_retailprice) over(partition by p_mfgr order by p_retailprice range between 5 preceding and current row) ,
sum(p_retailprice) over(partition by p_mfgr order by p_retailprice range between 5 preceding and current row)
from part_dec;