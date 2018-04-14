--! qt:dataset:part
-- corr var are only supported in where/having clause

select * from part po where p_size IN (select p_size from (select p_size, p_type from part pp where pp.p_brand = po.p_brand) p where p.p_type=po.p_type) ;
