create table datatype_stats_n0(
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
        bin BINARY)
PARTITIONED BY (t TINYINT);

INSERT INTO datatype_stats_n0 values(3, 45, 456, 45454.4, 454.6565, 2355, '2012-01-01 01:02:03', '2012-01-01', 'update_statistics', 'stats', 'hive', 'true', 'bin', 2);
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

INSERT INTO datatype_stats_n0 values(2, 44, 455, 45454.3, 454.6564, 2354, '2012-01-01 01:02:02', '2011-12-31', 'update_statistic', 'stat', 'hi', 'false', 'bi', 1);

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

INSERT INTO datatype_stats_n0 values(4, 46, 457, 45454.5, 454.6566, 2356, '2012-01-01 01:02:04', '2012-01-02', 'update_statisticsss', 'statsss', 'hiveee', 'true', 'binnn', 4);

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

set hive.stats.max.num.stats=2;
ANALYZE TABLE datatype_stats_n0 PARTITION(t) COMPUTE STATISTICS FOR COLUMNS;
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