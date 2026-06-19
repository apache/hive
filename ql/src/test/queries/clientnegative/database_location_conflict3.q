CREATE DATABASE db
MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db';

ALTER DATABASE db SET LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db';
