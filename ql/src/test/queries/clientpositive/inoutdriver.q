create table test_n3 (a int) stored as inputformat 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'RCFileInDriver' outputdriver 'RCFileOutDriver';
desc extended test_n3;
