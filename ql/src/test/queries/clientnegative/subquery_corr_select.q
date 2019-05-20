--! qt:dataset:part
-- correlated var is only allowed in where/having
explain select * from part po where p_size IN (select po.p_size from part p where p.p_type=po.p_type) ;
