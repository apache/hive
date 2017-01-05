explain select * from part where p_partkey IN (select count(*) from part pp where pp.p_type = part.p_type);
