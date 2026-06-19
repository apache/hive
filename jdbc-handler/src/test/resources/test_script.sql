DROP TABLE IF EXISTS test_strategy;

CREATE TABLE IF NOT EXISTS test_strategy (
  strategy_id int(11) NOT NULL,
  name varchar(50) NOT NULL,
  referrer varchar(1024) DEFAULT NULL,
  landing varchar(1024) DEFAULT NULL,
  priority int(11) DEFAULT NULL,
  implementation varchar(512) DEFAULT NULL,
  last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (strategy_id)
);


INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (1,'S1','aaa','abc',1000,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (2,'S2','bbb','def',990,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (3,'S3','ccc','ghi',1000,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (4,'S4','ddd','jkl',980,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (5,'S5','eee',NULL,NULL,NULL,'2012-05-08 15:01:15');

CREATE TABLE IF NOT EXISTS all_types_table (
    char_col CHAR,
    char_with_len_col CHAR(20),
    char_large_col CHARACTER LARGE OBJECT,
    varchar_col VARCHAR,
    varchar_with_len_col VARCHAR(1024),
    varchar_ci_col VARCHAR_IGNORECASE,
    bool_col BOOLEAN,
    tiny_col TINYINT,
    small_col SMALLINT,
    int_col INTEGER,
    big_col BIGINT,
    numeric_col NUMERIC,
    numeric_with_prec_scale_col NUMERIC(9,3),
    real_col REAL,
    double_col DOUBLE PRECISION,
    float_col DECFLOAT,
    date_col DATE,
    time_col TIME,
    time_zone_col TIME WITH TIME ZONE,
    timestamp_col TIMESTAMP,
    timestamp_zone_col TIMESTAMP WITH TIME ZONE,
    interval_col INTERVAL YEAR,
    binary_col BINARY,
    binary_var_col BINARY VARYING,
    binarl_large_col BINARY LARGE OBJECT,
    java_col JAVA_OBJECT,
    enum_col ENUM('clubs', 'diamonds', 'hearts', 'spades'),
    geo_col GEOMETRY,
    json_col JSON,
    uuid_col UUID,
    array_col INTEGER ARRAY,
    row_col ROW(streen_num INTEGER, street_name CHAR(20))
);
