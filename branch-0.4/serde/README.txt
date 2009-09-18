What is SerDe and how to write a SerDe?
------------------------
Please refer to http://wiki.apache.org/hadoop/Hive/DeveloperGuide Section SerDe.

How to load data into Hive
------------------------
In order to load data into Hive, we need to tell Hive the format of the data
through "CREATE TABLE" statement:

* FileFormat: the data has to be in Text or SequenceFile.
* Format of the row:
  * If the data is in delimited format, use MetadataTypedColumnsetSerDe
  * If the data is in delimited format and has more than 1 levels of delimitor,
    use DynamicSerDe with TCTLSeparatedProtocol
  * If the data is a serialized thrift object, use ThriftSerDe

The steps to load the data:
1 Create a table:

  CREATE TABLE t (foo STRING, bar STRING)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE;

  CREATE TABLE t2 (foo STRING, bar ARRAY<STRING>)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  COLLECTION ITEMS TERMINATED BY ','
  STORED AS TEXTFILE;

  CREATE TABLE t3 (foo STRING, bar MAP<STRING,STRING>)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  COLLECTION ITEMS TERMINATED BY ','
  MAP KEYS TERMINATED BY ':'
  STORED AS TEXTFILE;

  CREATE TABLE t4 (foo STRING, bar MAP<STRING,STRING>)
  ROW FORMAT SERIALIZER 'org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe'
  WITH SERDEPROPERTIES ('columns'='foo,bar','SERIALIZATION.FORMAT'='9');

  (RegexDeserializer is not done yet)
  CREATE TABLE t5 (foo STRING, bar STRING)
  ROW FORMAT SERIALIZER 'org.apache.hadoop.hive.serde2.RegexDeserializer'
  WITH SERDEPROPERTIES ('regex'='([a-z]*) *([a-z]*)');

2 Load the data:
  LOAD DATA LOCAL INPATH '../examples/files/kv1.txt' OVERWRITE INTO TABLE t;



How to read data from Hive tables
------------------------
In order to read data from Hive tables, we need to know the same 2 things:
* File Format
* Row Format

Then we just need to directly open the HDFS file and read the data.


