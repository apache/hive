DROP TABLE nopart_load;
CREATE TABLE nopart_load(a STRING, b STRING) PARTITIONED BY (ds STRING);

load data local inpath '../data/files/kv1.txt' overwrite into table nopart_load ;

DROP TABLE nopart_load;