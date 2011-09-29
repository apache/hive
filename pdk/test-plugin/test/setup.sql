create table onerow(s string);
load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/onerow.txt'
overwrite into table onerow;
