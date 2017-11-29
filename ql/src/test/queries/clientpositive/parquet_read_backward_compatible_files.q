-- This test makes sure that parquet can read older parquet files written by Hive <= 0.12
-- alltypesparquet is a files written by older version of Hive

CREATE TABLE alltypesparquet_old (
    bo1 boolean,
    ti1 tinyint,
    si1 smallint,
    i1 int,
    bi1 bigint,
    f1 float,
    d1 double,
    s1 string,
    m1 map<string,string>,
    l1 array<int>,
    st1 struct<c1:int,c2:string>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/alltypesparquet' OVERWRITE INTO TABLE alltypesparquet_old;

SELECT * FROM alltypesparquet_old;
