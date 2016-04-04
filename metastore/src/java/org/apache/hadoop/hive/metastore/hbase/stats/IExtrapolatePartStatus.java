package org.apache.hadoop.hive.metastore.hbase.stats;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;

public interface IExtrapolatePartStatus {
  // The following function will extrapolate the stats when the column stats of
  // some partitions are missing.
  /**
   * @param extrapolateData
   *          it will carry back the specific stats, e.g., DOUBLE_STATS or
   *          LONG_STATS
   * @param numParts
   *          the total number of partitions
   * @param numPartsWithStats
   *          the number of partitions that have stats
   * @param adjustedIndexMap
   *          the partition name to index map
   * @param adjustedStatsMap
   *          the partition name to its stats map
   * @param densityAvg
   *          the average of ndv density, which is useful when
   *          useDensityFunctionForNDVEstimation is true.
   */
  public abstract void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg);

}
