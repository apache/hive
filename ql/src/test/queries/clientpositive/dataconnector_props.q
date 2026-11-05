-- DESCRIBE CONNECTOR EXTENDED renders the connector parameter map.
-- DCPROPERTIES carries the DBCP connection properties for the remote source; these are
-- the same keys that are dropped from explain output (HIVE-28838), alongside ordinary
-- connector tuning parameters that are expected to round-trip.
CREATE CONNECTOR IF NOT EXISTS mysql_props_dc
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1",
"hive.connector.autoReconnect"="true",
"hive.connector.maxReconnects"="3");

DESCRIBE CONNECTOR mysql_props_dc;
DESCRIBE CONNECTOR EXTENDED mysql_props_dc;

-- clean up so it won't affect other tests
DROP CONNECTOR mysql_props_dc;
