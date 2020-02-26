package org.apache.hadoop.hive.ql.stats.estimator;

import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;

/**
 * Combines {@link ColStatistics} objects to provide the most pessimistic estimate.
 */
public class PessimisticStatCombiner {

  private boolean inited;
  private ColStatistics result;

  public void add(ColStatistics stat) {
    if (!inited) {
      inited = true;
      result = stat.clone();
      result.setRange(null);
      result.setIsEstimated(true);
      return;
    } else {
      if (stat.getAvgColLen() > result.getAvgColLen()) {
        result.setAvgColLen(stat.getAvgColLen());
      }
      if (stat.getCountDistint() > result.getCountDistint()) {
        result.setCountDistint(stat.getCountDistint());
      }
      if (stat.getNumNulls() > result.getNumNulls()) {
        result.setNumNulls(stat.getNumNulls());
      }
      if (stat.getNumTrues() > result.getNumTrues()) {
        result.setNumTrues(stat.getNumTrues());
      }
      if (stat.getNumFalses() > result.getNumFalses()) {
        result.setNumFalses(stat.getNumFalses());
      }
      if (stat.isFilteredColumn()) {
        result.setFilterColumn();
      }

    }

  }
  public Optional<ColStatistics> getResult() {
    return Optional.of(result);

  }
}