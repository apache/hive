-- subqueries in UDFs are not allowed
explain SELECT count((SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)) from part;