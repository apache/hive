CREATE TABLE variant_mv_src (id INT, s STRING);
CREATE MATERIALIZED VIEW variant_mv DISABLE REWRITE AS SELECT id, parse_json(s) AS v FROM variant_mv_src;
