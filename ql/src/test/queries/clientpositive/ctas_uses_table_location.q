--! qt:dataset:src
CREATE TABLE ctas_table_with_loc LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/ctas_table_with_loc' AS SELECT * FROM default.src;
