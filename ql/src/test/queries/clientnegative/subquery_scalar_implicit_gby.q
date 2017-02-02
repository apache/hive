select * from part where p_size <> (select count(p_size) from part pp where part.p_type <> pp.p_type);
