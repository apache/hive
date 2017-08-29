set hive.mapred.mode=nonstrict;
CREATE TABLE staging(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging;

CREATE TABLE orc_ppd(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd select * from staging;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;

select * from orc_ppd limit 1;

alter table orc_ppd set tblproperties("orc.bloom.filter.fpp"="0.01");

insert overwrite table orc_ppd select * from staging;

select * from orc_ppd limit 1;

CREATE TABLE orc_ppd_part(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
PARTITIONED BY (ds string, hr int) STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd_part partition(ds = "2015", hr = 10) select * from staging;

select * from orc_ppd_part limit 1;
