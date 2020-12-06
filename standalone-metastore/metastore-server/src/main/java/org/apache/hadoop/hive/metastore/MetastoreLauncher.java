package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;

public interface MetastoreLauncher extends Runnable{
  MetastoreLauncher configure(Configuration conf, HiveMetaStore.HiveMetastoreCli cli);
}
