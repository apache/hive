--! qt:dataset:part

-- TODO: Turn vectorization on after fixing HIVE-24595
set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
select p_name from part where p_size > (select p_size from part);
