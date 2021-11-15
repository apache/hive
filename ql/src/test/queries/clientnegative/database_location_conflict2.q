CREATE DATABASE db
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db';

ALTER DATABASE db SET MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db';

