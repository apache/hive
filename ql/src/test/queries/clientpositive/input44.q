--! qt:dataset:src
CREATE TABLE dest_n0(key INT, value STRING) STORED AS TEXTFILE;

SET hive.output.file.extension=.txt;
INSERT OVERWRITE TABLE dest_n0 SELECT src.* FROM src;

dfs -cat ${system:test.warehouse.dir}/dest_n0/*.txt
