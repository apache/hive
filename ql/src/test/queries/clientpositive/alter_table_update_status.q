--! qt:dataset:src1
create table src_stat_n0 as select * from src1;

create table src_stat_int_n0 (
  key         double,
  value       string
);

LOAD DATA LOCAL INPATH '../../data/files/kv3.txt' INTO TABLE src_stat_int_n0;

ANALYZE TABLE src_stat_n0 COMPUTE STATISTICS for columns key;

describe formatted src_stat_n0 key;

ALTER TABLE src_stat_n0 UPDATE STATISTICS for column key SET ('numDVs'='1111','avgColLen'='1.111');

describe formatted src_stat_n0 key;

ALTER TABLE src_stat_n0 UPDATE STATISTICS for column value SET ('numDVs'='121','numNulls'='122','avgColLen'='1.23','maxColLen'='124');

describe formatted src_stat_n0 value;

ANALYZE TABLE src_stat_int_n0 COMPUTE STATISTICS for columns key;

describe formatted src_stat_int_n0 key;

ALTER TABLE src_stat_int_n0 UPDATE STATISTICS for column key SET ('numDVs'='2222','lowValue'='333.22','highValue'='22.22');

describe formatted src_stat_int_n0 key;



create database if not exists dummydb;

use dummydb;

ALTER TABLE default.src_stat_n0 UPDATE STATISTICS for column key SET ('numDVs'='3333','avgColLen'='2.222');

describe formatted default.src_stat_n0 key;

ALTER TABLE default.src_stat_n0 UPDATE STATISTICS for column value SET ('numDVs'='232','numNulls'='233','avgColLen'='2.34','maxColLen'='235');

describe formatted default.src_stat_n0 value;

use default;

drop database dummydb;

create table datatype_stats_n0(
        t TINYINT,
        s SMALLINT,
        i INT,
        b BIGINT,
        f FLOAT,
        d DOUBLE,
        dem DECIMAL, --default decimal (10,0)
        ts TIMESTAMP,
        dt DATE,
        str STRING,
        v VARCHAR(12),
        c CHAR(5),
        bl BOOLEAN,
        bin BINARY);

INSERT INTO datatype_stats_n0 values(2, 3, 45, 456, 45454.4, 454.6565, 2355, '2012-01-01 01:02:03', '2012-01-01', 'update_statistics', 'stats', 'hive', 'true', 'bin');
INSERT INTO datatype_stats_n0 values(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
DESC FORMATTED datatype_stats_n0 s;
DESC FORMATTED datatype_stats_n0 i;
DESC FORMATTED datatype_stats_n0 b;
DESC FORMATTED datatype_stats_n0 f;
DESC FORMATTED datatype_stats_n0 d;
DESC FORMATTED datatype_stats_n0 dem;
DESC FORMATTED datatype_stats_n0 ts;
DESC FORMATTED datatype_stats_n0 dt;
DESC FORMATTED datatype_stats_n0 str;
DESC FORMATTED datatype_stats_n0 v;
DESC FORMATTED datatype_stats_n0 c;
DESC FORMATTED datatype_stats_n0 bl;
DESC FORMATTED datatype_stats_n0 bin;

--tinyint
DESC FORMATTED datatype_stats_n0 t;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column t SET ('numDVs'='232','numNulls'='233','highValue'='234','lowValue'='35');
DESC FORMATTED datatype_stats_n0 t;
--smallint
DESC FORMATTED datatype_stats_n0 s;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column s SET ('numDVs'='56','numNulls'='56','highValue'='489','lowValue'='25');
DESC FORMATTED datatype_stats_n0 s;
--int
DESC FORMATTED datatype_stats_n0 i;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column i SET ('numDVs'='59','numNulls'='1','highValue'='889','lowValue'='5');
DESC FORMATTED datatype_stats_n0 i;
--bigint
DESC FORMATTED datatype_stats_n0 b;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column b SET ('numDVs'='9','numNulls'='14','highValue'='89','lowValue'='8');
DESC FORMATTED datatype_stats_n0 b;

--float
DESC FORMATTED datatype_stats_n0 f;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column f SET ('numDVs'='563','numNulls'='45','highValue'='2345.656','lowValue'='8.00');
DESC FORMATTED datatype_stats_n0 f;
--double
DESC FORMATTED datatype_stats_n0 d;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column d SET ('numDVs'='5677','numNulls'='12','highValue'='560.3367','lowValue'='0.00455');
DESC FORMATTED datatype_stats_n0 d;
--decimal
DESC FORMATTED datatype_stats_n0 dem;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column dem SET ('numDVs'='57','numNulls'='912','highValue'='560','lowValue'='0');
DESC FORMATTED datatype_stats_n0 dem;
--timestamp
DESC FORMATTED datatype_stats_n0 ts;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column ts SET ('numDVs'='7','numNulls'='12','highValue'='1357030923','lowValue'='1357030924');
DESC FORMATTED datatype_stats_n0 ts;
--decimal
DESC FORMATTED datatype_stats_n0 dt;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column dt SET ('numDVs'='57','numNulls'='912','highValue'='2012-01-01','lowValue'='2001-02-04');
DESC FORMATTED datatype_stats_n0 dt;
--string
DESC FORMATTED datatype_stats_n0 str;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column str SET ('numDVs'='232','numNulls'='233','avgColLen'='2.34','maxColLen'='235');
DESC FORMATTED datatype_stats_n0 str;
--varchar
DESC FORMATTED datatype_stats_n0 v;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column v SET ('numDVs'='22','numNulls'='33','avgColLen'='4.40','maxColLen'='25');
DESC FORMATTED datatype_stats_n0 v;
--char
DESC FORMATTED datatype_stats_n0 c;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column c SET ('numDVs'='2','numNulls'='03','avgColLen'='9.00','maxColLen'='58');
DESC FORMATTED datatype_stats_n0 c;
--boolean
DESC FORMATTED datatype_stats_n0 bl;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column bl SET ('numNulls'='1','numTrues'='9','numFalses'='8');
DESC FORMATTED datatype_stats_n0 bl;
--binary
DESC FORMATTED datatype_stats_n0 bin;
ALTER TABLE default.datatype_stats_n0 UPDATE STATISTICS for column bin SET ('numNulls'='8','avgColLen'='2.0','maxColLen'='8');
DESC FORMATTED datatype_stats_n0 bin;

