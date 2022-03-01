--! qt:dataset:src
-- invalid argument type
SELECT concat_ws(',', array(1, 2, 3)) FROM src LIMIT 1;
