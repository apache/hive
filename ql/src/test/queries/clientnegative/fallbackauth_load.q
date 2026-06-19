set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

!cp ../../data/files/kv1.txt .;

create table fallbackauthload(c1 string, c2 string);

!chmod 777 kv1.txt;
load data local inpath 'kv1.txt' into table fallbackauthload;

!chmod 755 kv1.txt;
load data local inpath 'kv1.txt' into table fallbackauthload;

!rm kv1.txt;
drop table fallbackauthload;
