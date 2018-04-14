--! qt:dataset:src
SELECT concat(value, concat(value)) FROM src GROUP BY concat(value);
