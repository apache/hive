-- explain plan json:  the query gets the formatted json output of the query plan of the hive query

-- JAVA_VERSION_SPECIFIC_OUTPUT

EXPLAIN FORMATTED SELECT count(1) FROM src;
