package org.apache.hadoop.hive.ql.stats;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

// FIXME missing apidoc
public interface StatEstimator {

  // FIXME missing apidoc
  public Optional<ColStatistics> estimate(GenericUDF genericUDF, List<ColStatistics> csList);
}
