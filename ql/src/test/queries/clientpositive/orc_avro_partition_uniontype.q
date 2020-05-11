--! qt:dataset:src
-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
SET hive.exec.schema.evolution = false;

CREATE TABLE avro_orc_partitioned_uniontype (a uniontype<boolean, string>) PARTITIONED BY (b int) STORED AS ORC;

INSERT INTO avro_orc_partitioned_uniontype PARTITION (b=1) SELECT create_union(1, true, value) FROM src;

ALTER TABLE avro_orc_partitioned_uniontype SET FILEFORMAT AVRO;

SELECT * FROM avro_orc_partitioned_uniontype;
