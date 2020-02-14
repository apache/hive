package org.apache.hadoop.hive.ql.udf.generic;

import java.util.Optional;

import org.apache.hadoop.hive.ql.stats.IStatEstimator;

//FIXME move
public interface IStatEstimatorProvider {

  public Optional<IStatEstimator> getStatEstimator();
}
