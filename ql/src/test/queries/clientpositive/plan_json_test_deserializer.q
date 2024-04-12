--! qt:dataset:src
-- explain plan json:  the query gets the formatted json output of the query plan of the hive query
set hive.test.cbo.plan.serialization.deserialization.enabled=true;

EXPLAIN FORMATTED SELECT count(1) FROM src;
