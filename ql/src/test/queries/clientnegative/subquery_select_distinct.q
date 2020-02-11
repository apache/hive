--! qt:dataset:part
-- subqueries in UDFs are not allowed
explain SELECT distinct (SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type) from part;