PREHOOK: query: -- Currently, double to smallint conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key double, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')
PREHOOK: type: CREATETABLE
PREHOOK: Output: database:default
PREHOOK: Output: default@src_orc
POSTHOOK: query: -- Currently, double to smallint conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key double, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')
POSTHOOK: type: CREATETABLE
POSTHOOK: Output: database:default
POSTHOOK: Output: default@src_orc
PREHOOK: query: alter table src_orc change key key smallint
PREHOOK: type: ALTERTABLE_RENAMECOL
PREHOOK: Input: default@src_orc
PREHOOK: Output: default@src_orc
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions :
key
