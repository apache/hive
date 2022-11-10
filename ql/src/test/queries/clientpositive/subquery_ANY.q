--! qt:dataset:part
-- SORT_QUERY_RESULTS

CREATE TABLE part_null_n1 as select * from part;
insert into part_null_n1 values(17273,NULL,NULL,NULL,NULL, NULL, NULL,NULL,NULL);

-- multi
explain select * from part_null_n1 where p_name IN (select p_name from part where part.p_type = part_null_n1.p_type)
                    AND p_size >= ANY(select p_size from part pp where part_null_n1.p_type = pp.p_type);

DROP TABLE part_null_n1;
DROP TABLE part_null_n0;
DROP TABLE tempty;
