SET hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
CREATE TABLE t (a INT);
EXPLAIN ALTER TABLE t SET OWNER USER user1;
ALTER TABLE t SET OWNER USER user1;
