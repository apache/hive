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

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/tiny/part.tbl.bz2' overwrite into table part;

analyze table part compute statistics;
analyze table part compute statistics for columns;
