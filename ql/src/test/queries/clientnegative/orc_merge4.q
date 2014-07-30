DROP TABLE orcfile_merge;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE orcfile_merge (key INT, value STRING)
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

INSERT OVERWRITE TABLE orcfile_merge PARTITION(ds,part) SELECT * FROM srcpart;

ALTER TABLE orcfile_merge ADD COLUMNS (newkey int);

INSERT INTO TABLE orcfile_merge PARTITION(ds,part) SELECT key,value,key,ds,hr FROM srcpart;

-- will fail because of different column count
ALTER TABLE orcfile_merge PARTITION(ds='2008-04-08',part=11) CONCATENATE;

DROP TABLE orcfile_merge;
