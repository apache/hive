package org.apache.hadoop.hive.ql.stats.estimator;

import java.util.Optional;

//FIXME move
public interface IStatEstimatorProvider {

  public Optional<IStatEstimator> getStatEstimator();
}
