
-- since subquery is not top level expression this should throw an error
explain SELECT p_size, 1+(SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type) from part;