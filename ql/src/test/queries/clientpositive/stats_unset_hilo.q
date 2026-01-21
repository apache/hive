-- test tables prep
-- Both min and max stats UNSET
CREATE TABLE stats_both_unset (
  col_tinyint TINYINT,
  col_smallint SMALLINT,
  col_int INT,
  col_bigint BIGINT,
  col_float FLOAT,
  col_double DOUBLE
);
ALTER TABLE stats_both_unset UPDATE STATISTICS SET('numRows'='10000');
ALTER TABLE stats_both_unset UPDATE STATISTICS FOR COLUMN col_tinyint SET('numDVs'='100','numNulls'='0');
ALTER TABLE stats_both_unset UPDATE STATISTICS FOR COLUMN col_smallint SET('numDVs'='100','numNulls'='0');
ALTER TABLE stats_both_unset UPDATE STATISTICS FOR COLUMN col_int SET('numDVs'='100','numNulls'='0');
ALTER TABLE stats_both_unset UPDATE STATISTICS FOR COLUMN col_bigint SET('numDVs'='100','numNulls'='0');
ALTER TABLE stats_both_unset UPDATE STATISTICS FOR COLUMN col_float SET('numDVs'='100','numNulls'='0');
ALTER TABLE stats_both_unset UPDATE STATISTICS FOR COLUMN col_double SET('numDVs'='100','numNulls'='0');

-- Only min SET, max UNSET
CREATE TABLE stats_min_only (
  col_tinyint TINYINT,
  col_smallint SMALLINT,
  col_int INT,
  col_bigint BIGINT,
  col_float FLOAT,
  col_double DOUBLE
);
ALTER TABLE stats_min_only UPDATE STATISTICS SET('numRows'='10000');
ALTER TABLE stats_min_only UPDATE STATISTICS FOR COLUMN col_tinyint SET('numDVs'='100','numNulls'='0','lowValue'='-10');
ALTER TABLE stats_min_only UPDATE STATISTICS FOR COLUMN col_smallint SET('numDVs'='100','numNulls'='0','lowValue'='100');
ALTER TABLE stats_min_only UPDATE STATISTICS FOR COLUMN col_int SET('numDVs'='100','numNulls'='0','lowValue'='-1000');
ALTER TABLE stats_min_only UPDATE STATISTICS FOR COLUMN col_bigint SET('numDVs'='100','numNulls'='0','lowValue'='10000');
ALTER TABLE stats_min_only UPDATE STATISTICS FOR COLUMN col_float SET('numDVs'='100','numNulls'='0','lowValue'='-10.5');
ALTER TABLE stats_min_only UPDATE STATISTICS FOR COLUMN col_double SET('numDVs'='100','numNulls'='0','lowValue'='100.5');

-- Only max SET, min UNSET
CREATE TABLE stats_max_only (
  col_tinyint TINYINT,
  col_smallint SMALLINT,
  col_int INT,
  col_bigint BIGINT,
  col_float FLOAT,
  col_double DOUBLE
);
ALTER TABLE stats_max_only UPDATE STATISTICS SET('numRows'='10000');
ALTER TABLE stats_max_only UPDATE STATISTICS FOR COLUMN col_tinyint SET('numDVs'='100','numNulls'='0','highValue'='-5');
ALTER TABLE stats_max_only UPDATE STATISTICS FOR COLUMN col_smallint SET('numDVs'='100','numNulls'='0','highValue'='1000');
ALTER TABLE stats_max_only UPDATE STATISTICS FOR COLUMN col_int SET('numDVs'='100','numNulls'='0','highValue'='-500');
ALTER TABLE stats_max_only UPDATE STATISTICS FOR COLUMN col_bigint SET('numDVs'='100','numNulls'='0','highValue'='100000');
ALTER TABLE stats_max_only UPDATE STATISTICS FOR COLUMN col_float SET('numDVs'='100','numNulls'='0','highValue'='-25.5');
ALTER TABLE stats_max_only UPDATE STATISTICS FOR COLUMN col_double SET('numDVs'='100','numNulls'='0','highValue'='1000.5');

-- actual test groups
DESCRIBE FORMATTED stats_both_unset col_tinyint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_both_unset WHERE col_tinyint > 50;
DESCRIBE FORMATTED stats_both_unset col_smallint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_both_unset WHERE col_smallint < 500;
DESCRIBE FORMATTED stats_both_unset col_int;
EXPLAIN EXTENDED SELECT count(1) FROM stats_both_unset WHERE col_int BETWEEN 100 AND 500;
DESCRIBE FORMATTED stats_both_unset col_bigint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_both_unset WHERE col_bigint = 999;
DESCRIBE FORMATTED stats_both_unset col_float;
EXPLAIN EXTENDED SELECT count(1) FROM stats_both_unset WHERE col_float > 50.0;
DESCRIBE FORMATTED stats_both_unset col_double;
EXPLAIN EXTENDED SELECT count(1) FROM stats_both_unset WHERE col_double < 500.0;

DESCRIBE FORMATTED stats_min_only col_tinyint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_min_only WHERE col_tinyint > 50;
DESCRIBE FORMATTED stats_min_only col_smallint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_min_only WHERE col_smallint > 200;
DESCRIBE FORMATTED stats_min_only col_int;
EXPLAIN EXTENDED SELECT count(1) FROM stats_min_only WHERE col_int > 2000;
DESCRIBE FORMATTED stats_min_only col_bigint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_min_only WHERE col_bigint > 20000;
DESCRIBE FORMATTED stats_min_only col_float;
EXPLAIN EXTENDED SELECT count(1) FROM stats_min_only WHERE col_float > 50.0;
DESCRIBE FORMATTED stats_min_only col_double;
EXPLAIN EXTENDED SELECT count(1) FROM stats_min_only WHERE col_double > 200.0;

DESCRIBE FORMATTED stats_max_only col_tinyint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_max_only WHERE col_tinyint < 50;
DESCRIBE FORMATTED stats_max_only col_smallint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_max_only WHERE col_smallint < 500;
DESCRIBE FORMATTED stats_max_only col_int;
EXPLAIN EXTENDED SELECT count(1) FROM stats_max_only WHERE col_int < 5000;
DESCRIBE FORMATTED stats_max_only col_bigint;
EXPLAIN EXTENDED SELECT count(1) FROM stats_max_only WHERE col_bigint < 50000;
DESCRIBE FORMATTED stats_max_only col_float;
EXPLAIN EXTENDED SELECT count(1) FROM stats_max_only WHERE col_float < 50.0;
DESCRIBE FORMATTED stats_max_only col_double;
EXPLAIN EXTENDED SELECT count(1) FROM stats_max_only WHERE col_double < 500.0;
