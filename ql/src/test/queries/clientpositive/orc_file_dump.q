set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

CREATE TABLE staging_n4(t tinyint,
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

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging_n4;

CREATE TABLE orc_ppd_n0(t tinyint,
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

insert overwrite table orc_ppd_n0 select * from staging_n4;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;

select * from orc_ppd_n0 limit 1;

alter table orc_ppd_n0 set tblproperties("orc.bloom.filter.fpp"="0.01");

insert overwrite table orc_ppd_n0 select * from staging_n4;

select * from orc_ppd_n0 limit 1;

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

insert overwrite table orc_ppd_part partition(ds = "2015", hr = 10) select * from staging_n4;

select * from orc_ppd_part limit 1;
