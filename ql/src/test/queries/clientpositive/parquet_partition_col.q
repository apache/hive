-- The peculiarity of this test is that the partitioning column exists inside each individual Parquet
-- file (under data/files/parquet_partition) and at the same time it is also present in the directory
-- structure.
-- 
-- The schema of the Parquet files are shown below:
-- {
--   "type" : "record",
--   "name" : "hive_schema",
--   "fields" : [ {
--     "name" : "strcol",
--     "type" : [ "null", "string" ],
--     "default" : null
--   }, {
--     "name" : "intcol",
--     "type" : [ "null", "int" ],
--     "default" : null
--   }, {
--     "name" : "pcol",
--     "type" : [ "null", "int" ],
--     "default" : null
--   } ]
-- }
-- The test case necessitates the table to be external with location already specified; we don't
-- want the data to be reloaded cause it will change the actual problem.

create external table test(
  strcol string,
  intcol integer
) partitioned by (pcol int)
stored as parquet
location '../../data/files/parquet_partition';

msck repair table test;

select * from test where pcol=100 and intcol=2;
select * from test where PCOL=200 and intcol=3;
select * from test where `pCol`=300 and intcol=5;
