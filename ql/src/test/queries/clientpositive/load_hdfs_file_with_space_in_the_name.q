dfs -mkdir hdfs:///tmp/test/load_file_with_space_in_the_name;

dfs -copyFromLocal ../data/files hdfs:///tmp/test/load_file_with_space_in_the_name;

CREATE TABLE load_file_with_space_in_the_name(name STRING, age INT);
LOAD DATA INPATH 'hdfs:///tmp/test/load_file_with_space_in_the_name/files/person age.txt' INTO TABLE load_file_with_space_in_the_name;

dfs -rmr hdfs:///tmp/test;

