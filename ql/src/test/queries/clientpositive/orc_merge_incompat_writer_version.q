set hive.vectorized.execution.enabled=false;

DROP TABLE part_orc;
CREATE TABLE part_orc(
  p_partkey int,
  p_name string,
  p_mfgr string,
  p_brand string,
  p_type string,
  p_size int,
  p_container string,
  p_retailprice double,
  p_comment string
)
STORED AS ORC;

-- writer version for this file is HIVE_13083
LOAD DATA LOCAL INPATH '../../data/files/part.orc' OVERWRITE INTO TABLE part_orc;

create table part_orc_staging as select * from part_orc;

-- will be written with current writer version
insert into table part_orc select * from part_orc_staging;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/part_orc/;
select sum(hash(*)) from part_orc;

-- will not be merged as writer version is not matching
ALTER TABLE part_orc CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/part_orc/;
select sum(hash(*)) from part_orc;
