--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;

set hive.auto.convert.join=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=255;

DROP TABLE orc_llap_part_temp;
DROP TABLE orc_llap_dim_part_temp;

CREATE TEMPORARY TABLE orc_llap_part_temp(
  csmallint SMALLINT,
  cint INT,
  cbigint BIGINT,
  cfloat FLOAT,
  cdouble DOUBLE,
  cstring1 STRING,
  cchar1 CHAR(255),
  cvchar1 VARCHAR(255),
  cboolean1 BOOLEAN,
  cboolean2 BOOLEAN
) PARTITIONED BY (ctinyint TINYINT) STORED AS ORC;

CREATE TEMPORARY TABLE orc_llap_dim_part_temp(
  csmallint SMALLINT,
  cint INT,
  cbigint BIGINT,
  cfloat FLOAT,
  cdouble DOUBLE,
  cstring1 STRING,
  cchar1 CHAR(255),
  cvchar1 VARCHAR(255),
  cboolean1 BOOLEAN,
  cboolean2 BOOLEAN
) PARTITIONED BY (ctinyint TINYINT) STORED AS ORC;

INSERT OVERWRITE TABLE orc_llap_part_temp PARTITION (ctinyint)
  SELECT csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring1, cstring1, cboolean1, cboolean2, ctinyint FROM alltypesorc;

INSERT OVERWRITE TABLE orc_llap_dim_part_temp PARTITION (ctinyint)
  SELECT null, null, sum(cbigint) as cbigint, null, null, null, null, null, null, null, ctinyint FROM alltypesorc WHERE ctinyint > 10 AND ctinyint < 21 GROUP BY ctinyint;

drop table llap_temp_table_n0_temp;

set hive.cbo.enable=false;
SET hive.llap.io.enabled=true;
SET hive.vectorized.execution.enabled=true;

explain vectorization detail
SELECT oft.ctinyint, oft.cint, oft.cchar1, oft.cvchar1 FROM orc_llap_part_temp oft
  INNER JOIN orc_llap_dim_part_temp od ON oft.ctinyint = od.ctinyint;
create temporary table llap_temp_table_n0_temp as
  SELECT oft.ctinyint, oft.cint, oft.cchar1, oft.cvchar1 FROM orc_llap_part_temp oft
    INNER JOIN orc_llap_dim_part_temp od ON oft.ctinyint = od.ctinyint;

explain vectorization detail
select sum(hash(*)) from llap_temp_table_n0_temp;
select sum(hash(*)) from llap_temp_table_n0_temp;
drop table llap_temp_table_n0_temp;


DROP TABLE orc_llap_part_temp;
DROP TABLE orc_llap_dim_part_temp;
