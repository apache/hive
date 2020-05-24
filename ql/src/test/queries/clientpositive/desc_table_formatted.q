--! qt:dataset:src1
CREATE TABLE datatype_stats_n0(
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

INSERT INTO datatype_stats_n0 values(1, 2, 44, 455, 45454.3, 454.6564, 2354, '2012-01-01 01:02:02', '2011-12-31', 'update_statisticr', 'statr', 'hivd', 'false', 'bin');
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
DESC FORMATTED datatype_stats_n0 t;
DESC FORMATTED datatype_stats_n0;
DESC datatype_stats_n0 s;
DESC datatype_stats_n0 i;
DESC datatype_stats_n0 b;
DESC datatype_stats_n0 f;
DESC datatype_stats_n0 d;
DESC datatype_stats_n0 dem;
DESC datatype_stats_n0 ts;
DESC datatype_stats_n0 dt;
DESC datatype_stats_n0 str;
DESC datatype_stats_n0 v;
DESC datatype_stats_n0 c;
DESC datatype_stats_n0 bl;
DESC datatype_stats_n0 bin;
DESC datatype_stats_n0;

SET hive.ddl.output.format=json;

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
DESC FORMATTED datatype_stats_n0 t;
DESC FORMATTED datatype_stats_n0;
DESC datatype_stats_n0 s;
DESC datatype_stats_n0 i;
DESC datatype_stats_n0 b;
DESC datatype_stats_n0 f;
DESC datatype_stats_n0 d;
DESC datatype_stats_n0 dem;
DESC datatype_stats_n0 ts;
DESC datatype_stats_n0 dt;
DESC datatype_stats_n0 str;
DESC datatype_stats_n0 v;
DESC datatype_stats_n0 c;
DESC datatype_stats_n0 bl;
DESC datatype_stats_n0 bin;
DESC datatype_stats_n0;
