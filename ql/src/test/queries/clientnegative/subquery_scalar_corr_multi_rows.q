--! qt:dataset:part
-- inner query produces more than one row

-- TODO: Turn vectorization on after fixing HIVE-24595
set hive.vectorized.execution.enabled=false;
select * from part where p_size >
    (select count(*) from part p where p.p_mfgr = part.p_mfgr group by p_type);