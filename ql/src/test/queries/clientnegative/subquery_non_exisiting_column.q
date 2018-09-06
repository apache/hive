--! qt:dataset:srcpart
--! qt:dataset:part

explain select * from srcpart where srcpart.key IN ( select p_type from part p where p.p_type = srcpart.non_exisiting_column)
and srcpart.key IN (select 4);