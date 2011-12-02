create table onerow(s string);
load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/onerow.txt'
overwrite into table onerow;
create table iris(
sepal_length string, sepal_width string,
petal_length string, petal_width string,
species string)
row format delimited fields terminated by '\t' stored as textfile;
load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/iris.txt'
overwrite into table iris;
