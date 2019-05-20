--! qt:dataset:part

select * from part where (select max(p_size) from part) IN (select p_size from part);