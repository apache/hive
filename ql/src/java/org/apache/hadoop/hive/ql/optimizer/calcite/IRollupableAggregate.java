package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.sql.SqlAggFunction;

public interface IRollupableAggregate {

  public SqlAggFunction getAggregate();
}
