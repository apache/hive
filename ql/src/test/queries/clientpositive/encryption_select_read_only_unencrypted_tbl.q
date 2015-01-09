-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS unencrypted_table;
CREATE TABLE unencrypted_table(key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/unencrypted_table';

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE unencrypted_table;

dfs -chmod -R 555 ${hiveconf:hive.metastore.warehouse.dir}/default/unencrypted_table;

SELECT count(*) FROM unencrypted_table;

drop table unencrypted_table;