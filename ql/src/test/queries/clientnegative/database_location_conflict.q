CREATE DATABASE db
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/db'
MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/db';

