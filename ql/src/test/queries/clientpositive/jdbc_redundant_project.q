--! qt:database:postgres:qdb:q_test_country_table_with_schema.postgres.sql

CREATE EXTERNAL TABLE country_bob (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = "country");

CREATE EXTERNAL TABLE country_alice (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "alice",
    "hive.sql.table" = "country");

EXPLAIN CBO
WITH cross_items AS (
  SELECT id
  FROM (
    SELECT id FROM country_bob
    INTERSECT
    SELECT id FROM country_alice
    INTERSECT
    SELECT id FROM country_bob
  ) x
)
SELECT channel, cnt
FROM (
  SELECT 'a' channel, COUNT(*) cnt
  FROM country_bob
  WHERE id IN (SELECT id FROM cross_items)
  UNION ALL
  SELECT 'b' channel, COUNT(*) cnt
  FROM country_alice
  WHERE id IN (SELECT id FROM cross_items)
  UNION ALL
  SELECT 'c' channel, COUNT(*) cnt
  FROM country_bob
  WHERE id IN (SELECT id FROM cross_items)
) y
ORDER BY channel;
