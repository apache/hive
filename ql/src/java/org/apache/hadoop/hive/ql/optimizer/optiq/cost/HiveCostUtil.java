package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.eigenbase.relopt.RelOptCost;

public class HiveCostUtil {
  private static final double cpuCostInNanoSec          = 1.0;
  private static final double netCostInNanoSec          = 150 * cpuCostInNanoSec;
  private static final double localFSWriteCostInNanoSec = 4 * netCostInNanoSec;
  private static final double localFSReadCostInNanoSec  = 4 * netCostInNanoSec;
  private static final double hDFSWriteCostInNanoSec    = 10 * localFSWriteCostInNanoSec;
  private static final double hDFSReadCostInNanoSec     = 1.5 * localFSReadCostInNanoSec;

  public static RelOptCost computCardinalityBasedCost(HiveRel hr) {
    return new HiveCost(hr.getRows(), 0, 0);
  }

  public static HiveCost computeCost(HiveTableScanRel t) {
    double cardinality = t.getRows();
    return new HiveCost(cardinality, 0, hDFSWriteCostInNanoSec * cardinality * 0);
  }
}
