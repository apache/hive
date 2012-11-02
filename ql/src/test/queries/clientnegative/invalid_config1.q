
set hive.internal.ddl.list.bucketing.enable=true;

CREATE TABLE skewedtable (key STRING, value STRING) SKEWED BY (key) ON (1,5,6);
