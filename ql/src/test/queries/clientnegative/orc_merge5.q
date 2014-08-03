DROP TABLE orcfile_merge;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.exec.orc.write.format=0.11;
CREATE TABLE orcfile_merge (key INT, value STRING)
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

INSERT OVERWRITE TABLE orcfile_merge PARTITION(ds,part) SELECT * FROM srcpart;

set hive.exec.orc.write.format=0.12;
INSERT INTO TABLE orcfile_merge PARTITION(ds,part) SELECT * FROM srcpart;

-- will fail because of different write format
ALTER TABLE orcfile_merge PARTITION(ds='2008-04-08',part=11) CONCATENATE;

DROP TABLE orcfile_merge;
