--! qt:dataset:src

explain
SELECT key, value, count(key) FROM src GROUP BY key, value with rollup;

SELECT key, value, count(key) FROM src GROUP BY key, value with rollup;
