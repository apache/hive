--! qt:dataset:part
select p_mfgr, p_name, p_size,
    sum(p_retailprice) over (range unbounded following) as s1
     from part distribute by p_mfgr sort by p_name;

