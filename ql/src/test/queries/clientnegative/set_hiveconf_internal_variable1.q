-- should fail: hive.conf.internal.variable.list is in restricted list
desc src;

set hive.conf.internal.variable.list=;
