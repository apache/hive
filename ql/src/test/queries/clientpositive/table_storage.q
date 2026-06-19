CREATE TABLE t (key STRING, val STRING);
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
ALTER TABLE t CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t INTO 3 BUCKETS;
ALTER TABLE t INTO 3 BUCKETS;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t NOT SORTED;
ALTER TABLE t NOT SORTED;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t NOT CLUSTERED;
ALTER TABLE t NOT CLUSTERED;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t SKEWED BY (key) ON (("a"), ("b")) STORED AS DIRECTORIES;
ALTER TABLE t SKEWED BY (key) ON (("a"), ("b")) STORED AS DIRECTORIES;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t SET SKEWED LOCATION ('a'='${hiveconf:hive.metastore.warehouse.dir}/t/key=a','b'='${hiveconf:hive.metastore.warehouse.dir}/t/key=b');
ALTER TABLE t SET SKEWED LOCATION ('a'='${hiveconf:hive.metastore.warehouse.dir}/t/key=a','b'='${hiveconf:hive.metastore.warehouse.dir}/t/key=b');
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t NOT SKEWED;
ALTER TABLE t NOT SKEWED;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t SET FILEFORMAT parquet;
ALTER TABLE t SET FILEFORMAT parquet;
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t SET LOCATION "file:///tmp/location";
ALTER TABLE t SET LOCATION "file:///tmp/location";
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t SET SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
ALTER TABLE t SET SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t SET SERDEPROPERTIES('property1'='value1', 'property2'='value2');
ALTER TABLE t SET SERDEPROPERTIES('property1'='value1', 'property2'='value2');
SHOW CREATE TABLE t;

EXPLAIN ALTER TABLE t UNSET SERDEPROPERTIES('property1');
ALTER TABLE t UNSET SERDEPROPERTIES('property1');
SHOW CREATE TABLE t;

-- it is valid to unset a non-existing property
ALTER TABLE t UNSET SERDEPROPERTIES('property1');
SHOW CREATE TABLE t;

-- a removed property can be set again
ALTER TABLE t SET SERDEPROPERTIES('property1'='value1');
SHOW CREATE TABLE t;

-- remove all serde properties
ALTER TABLE t UNSET SERDEPROPERTIES('property1', 'property2');
SHOW CREATE TABLE t;
