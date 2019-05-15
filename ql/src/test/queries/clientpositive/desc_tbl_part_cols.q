create table t1_n62 (a int, b string) partitioned by (c int, d string);
describe t1_n62;

set hive.display.partition.cols.separately=false;
describe t1_n62;

set hive.display.partition.cols.separately=true;
