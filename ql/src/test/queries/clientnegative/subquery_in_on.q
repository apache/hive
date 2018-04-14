--! qt:dataset:part
-- subquery in ON clause
explain SELECT p1.p_name FROM part p1 LEFT JOIN (select p_type as p_col from part ) p2
    ON (select pp1.p_type as p_col from part pp1 where pp1.p_partkey = p2.p_col);