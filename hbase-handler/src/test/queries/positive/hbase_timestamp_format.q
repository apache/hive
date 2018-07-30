
create external table hbase_str(rowkey string,mytime string,mystr string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'm:mytime,m:mystr')
  TBLPROPERTIES ('hbase.table.name' = 'hbase_ts', 'external.table.purge' = 'true');

describe hbase_str;
insert overwrite table hbase_str select key, '2001-02-03-04.05.06.123456', value from src limit 3;
select * from hbase_str;

-- Timestamp string does not match the default timestamp format, specify a custom timestamp format
create external table hbase_ts(rowkey string,mytime timestamp,mystr string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'm:mytime,m:mystr', 'timestamp.formats' = 'yyyy-MM-dd-HH.mm.ss.SSSSSS')
  TBLPROPERTIES ('hbase.table.name' = 'hbase_ts');

describe hbase_ts;
select * from hbase_ts;

drop table hbase_str;
drop table hbase_ts;
