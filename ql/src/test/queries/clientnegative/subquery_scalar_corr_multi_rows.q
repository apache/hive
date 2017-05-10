-- inner query produces more than one row
select * from part where p_size > (select count(*) from part p where p.p_mfgr = part.p_mfgr group by p_type);