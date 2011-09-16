SET hive.support.concurrency=false;

DROP TABLE IF EXISTS cf_demo_TBL;
--Test cassandra.cf.validatorType setting with counters
DROP TABLE cf_demo_TBL;
CREATE EXTERNAL TABLE cf_demo_TBL(row_key STRING, counterColumnLong INT)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
      WITH SERDEPROPERTIES ("cassandra.port" = "9170",
                            "cassandra.columns.mapping" = ":key,
                                                           counterColumnLong")                            
      TBLPROPERTIES ("cassandra.ks.name" = "counter_ks_demo",
                     "cassandra.slice.predicate.size" = "100",
                     "cassandra.cf.name" = "counter_cf_demo");

select row_key, counterColumnLong from cf_demo_TBL;

DROP TABLE cf_demo_TBL;
--Test LongType and IntegerType without using validatorType setting
CREATE EXTERNAL TABLE cf_demo_TBL(row_key STRING,
                                             uniqueid String,
                                             countLong BIGINT,
                                             countInt INT)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
      WITH SERDEPROPERTIES ("cassandra.port" = "9170",
                            "cassandra.columns.mapping" = ":key,
                                                           uniqueid,
                                                           countLong,
                                                           countInt")
      TBLPROPERTIES ("cassandra.ks.name" = "ks_demo",
                     "cassandra.slice.predicate.size" = "100",
                     "cassandra.cf.name" = "cf_demo");

--Skip uiniqueid as invalid characters will show up
select row_key, countLong, countInt from cf_demo_TBL;

--Test cassandra.cf.validatorType setting
DROP TABLE IF EXISTS cf_demo_TBL;
CREATE EXTERNAL TABLE cf_demo_TBL(row_key STRING,
                                             uniqueid String,
                                             countLong BIGINT,
                                             countInt INT)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
      WITH SERDEPROPERTIES ("cassandra.port" = "9170",
                            "cassandra.columns.mapping" = ":key,
                                                           uniqueid,
                                                           countLong,
                                                           countInt",
                            "cassandra.cf.validatorType" = "UTF8Type,
                                                            LexicalUUIDType,
                                                            LongType,
                                                            IntegerType")
      TBLPROPERTIES ("cassandra.ks.name" = "ks_demo",
                     "cassandra.slice.predicate.size" = "100",
                     "cassandra.cf.name" = "cf_demo");

select row_key, uniqueid, countLong, countInt from cf_demo_TBL;

--Test cassandra.cf.validatorType setting in different format
DROP TABLE IF EXISTS cf_demo_TBL;
CREATE EXTERNAL TABLE cf_demo_TBL(row_key STRING,
                                             uniqueid String,
                                             countLong BIGINT,
                                             countInt INT)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
      WITH SERDEPROPERTIES ("cassandra.port" = "9170",
                            "cassandra.columns.mapping" = ":key,
                                                           uniqueid,
                                                           countLong,
                                                           countInt",
                            "cassandra.cf.validatorType" = ",LexicalUUIDType,,IntegerType")
      TBLPROPERTIES ("cassandra.ks.name" = "ks_demo",
                     "cassandra.slice.predicate.size" = "100",
                     "cassandra.cf.name" = "cf_demo");

select row_key, uniqueid, countLong, countInt from cf_demo_TBL;

--Test super column family
DROP TABLE IF EXISTS super_demo_TBL;
CREATE EXTERNAL TABLE super_demo_TBL(row_key STRING,
                                     uniqueid String,
                                     countLong BIGINT,
                                     countInt INT)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
      WITH SERDEPROPERTIES ("cassandra.port" = "9170",
                            "cassandra.columns.mapping" = ":key,:column,:subcolumn,:value",
                            "cassandra.cf.validatorType" = "UTF8Type,UTF8Type,UTF8Type,LongType")
      TBLPROPERTIES ("cassandra.ks.name" = "super_ks_demo",
                     "cassandra.slice.predicate.size" = "100",
                     "cassandra.cf.name" = "super_cf_demo");

select row_key, uniqueid, countLong, countInt from super_demo_TBL;
