package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.plan.Context;
import org.apache.hadoop.hive.conf.HiveConf;


public class HiveConfigContext implements Context {
  private HiveConf config;

  public HiveConfigContext(HiveConf config) {
    this.config = config;
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(config)) {
      return clazz.cast(config);
    }
    return null;
  }
}