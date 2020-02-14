package org.apache.hadoop.hive.ql.udf.generic;

import java.util.Optional;

import org.apache.hadoop.hive.ql.stats.StatEstimator;

//FIXME move
public interface IStatEstimatorProvider {

  public Optional<StatEstimator> getStatEstimator();
}
