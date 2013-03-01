-- data setup
CREATE TABLE part( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

select p_mfgr, p_name, p_size,
    sum(p_retailprice) as s1 over (rows unbounded following)
     from part distribute by p_mfgr sort by p_name;

