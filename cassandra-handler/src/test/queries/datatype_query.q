SET hive.support.concurrency=false;

DROP TABLE cf_demo_TBL;
--Test LongType and IntegerType
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

select row_key, uniqueid, countLong, countInt from cf_demo_TBL;

DROP TABLE cf_demo_TBL;
