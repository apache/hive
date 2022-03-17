-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/

DROP TABLE if exists parquet_stats;

CREATE TABLE parquet_stats (
    id int,
    str string
) STORED AS PARQUET;

SET hive.stats.autogather=true;
INSERT INTO parquet_stats values(0, 'this is string 0'), (1, 'string 1');
DESC FORMATTED parquet_stats;

