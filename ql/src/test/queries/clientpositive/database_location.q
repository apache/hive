CREATE DATABASE db1;
DESCRIBE DATABASE EXTENDED db1;

USE db1;
CREATE TABLE table_db1_n0 (name STRING, value INT);

DESCRIBE FORMATTED table_db1_n0;
SHOW TABLES;

CREATE DATABASE db2
COMMENT 'database 2'
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db2';

DESCRIBE DATABASE EXTENDED db2;

USE db2;
CREATE TABLE table_db2 (name STRING, value INT);

DESCRIBE FORMATTED table_db2;
SHOW TABLES;

EXPLAIN
CREATE DATABASE db3
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3_ext'
MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3';

CREATE DATABASE db3
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3_ext'
MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3';

SHOW CREATE DATABASE db3;

DESCRIBE DATABASE db3;

EXPLAIN
ALTER DATABASE db3 SET LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3_ext_alt';

ALTER DATABASE db3 SET LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3_ext_alt';

DESCRIBE DATABASE db3;

EXPLAIN
ALTER DATABASE db3 SET MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3_alt';

ALTER DATABASE db3 SET MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db3_alt';

DESCRIBE DATABASE db3;


