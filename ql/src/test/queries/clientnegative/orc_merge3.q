DROP TABLE orcfile_merge;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.exec.orc.default.row.index.stride=1000;
CREATE TABLE orcfile_merge (key INT, value STRING)
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

INSERT OVERWRITE TABLE orcfile_merge PARTITION(ds,part) SELECT * FROM srcpart;

set hive.exec.orc.default.row.index.stride=2000;
INSERT INTO TABLE orcfile_merge PARTITION(ds,part) SELECT * FROM srcpart;

-- will fail because of different row index stride
ALTER TABLE orcfile_merge PARTITION(ds='2008-04-08',part=11) CONCATENATE;

DROP TABLE orcfile_merge;
