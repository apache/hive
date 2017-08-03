/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.columnstats.aggr;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongColumnStatsAggregator extends ColumnStatsAggregator implements
    IExtrapolatePartStatus {

  private static final Logger LOG = LoggerFactory.getLogger(LongColumnStatsAggregator.class);

  @Override
  public ColumnStatisticsObj aggregate(String colName, List<String> partNames,
      List<ColumnStatistics> css) throws MetaException {
    ColumnStatisticsObj statsObj = null;

    // check if all the ColumnStatisticsObjs contain stats and all the ndv are
    // bitvectors
    boolean doAllPartitionContainStats = partNames.size() == css.size();
    LOG.debug("doAllPartitionContainStats for " + colName + " is " + doAllPartitionContainStats);
    NumDistinctValueEstimator ndvEstimator = null;
    String colType = null;
    for (ColumnStatistics cs : css) {
      if (cs.getStatsObjSize() != 1) {
        throw new MetaException(
            "The number of columns should be exactly one in aggrStats, but found "
                + cs.getStatsObjSize());
      }
      ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
      if (statsObj == null) {
        colType = cso.getColType();
        statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType, cso
            .getStatsData().getSetField());
      }
      if (!cso.getStatsData().getLongStats().isSetBitVectors()
          || cso.getStatsData().getLongStats().getBitVectors().length() == 0) {
        ndvEstimator = null;
        break;
      } else {
        // check if all of the bit vectors can merge
        NumDistinctValueEstimator estimator = NumDistinctValueEstimatorFactory
            .getNumDistinctValueEstimator(cso.getStatsData().getLongStats().getBitVectors());
        if (ndvEstimator == null) {
          ndvEstimator = estimator;
        } else {
          if (ndvEstimator.canMerge(estimator)) {
            continue;
          } else {
            ndvEstimator = null;
            break;
          }
        }
      }
    }
    if (ndvEstimator != null) {
      ndvEstimator = NumDistinctValueEstimatorFactory
          .getEmptyNumDistinctValueEstimator(ndvEstimator);
    }
    LOG.debug("all of the bit vectors can merge for " + colName + " is " + (ndvEstimator != null));
    ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
    if (doAllPartitionContainStats || css.size() < 2) {
      LongColumnStatsData aggregateData = null;
      long lowerBound = 0;
      long higherBound = 0;
      double densityAvgSum = 0.0;
      for (ColumnStatistics cs : css) {
        ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
        LongColumnStatsData newData = cso.getStatsData().getLongStats();
        lowerBound = Math.max(lowerBound, newData.getNumDVs());
        higherBound += newData.getNumDVs();
        densityAvgSum += (newData.getHighValue() - newData.getLowValue()) / newData.getNumDVs();
        if (ndvEstimator != null) {
          ndvEstimator.mergeEstimators(NumDistinctValueEstimatorFactory
              .getNumDistinctValueEstimator(newData.getBitVectors()));
        }
        if (aggregateData == null) {
          aggregateData = newData.deepCopy();
        } else {
          aggregateData.setLowValue(Math.min(aggregateData.getLowValue(), newData.getLowValue()));
          aggregateData
              .setHighValue(Math.max(aggregateData.getHighValue(), newData.getHighValue()));
          aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
          aggregateData.setNumDVs(Math.max(aggregateData.getNumDVs(), newData.getNumDVs()));
        }
      }
      if (ndvEstimator != null) {
        // if all the ColumnStatisticsObjs contain bitvectors, we do not need to
        // use uniform distribution assumption because we can merge bitvectors
        // to get a good estimation.
        aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
      } else {
        long estimation;
        if (useDensityFunctionForNDVEstimation) {
          // We have estimation, lowerbound and higherbound. We use estimation
          // if it is between lowerbound and higherbound.
          double densityAvg = densityAvgSum / partNames.size();
          estimation = (long) ((aggregateData.getHighValue() - aggregateData.getLowValue()) / densityAvg);
          if (estimation < lowerBound) {
            estimation = lowerBound;
          } else if (estimation > higherBound) {
            estimation = higherBound;
          }
        } else {
          estimation = (long) (lowerBound + (higherBound - lowerBound) * ndvTuner);
        }
        aggregateData.setNumDVs(estimation);
      }
      columnStatisticsData.setLongStats(aggregateData);
    } else {
      // we need extrapolation
      LOG.debug("start extrapolation for " + colName);

      Map<String, Integer> indexMap = new HashMap<String, Integer>();
      for (int index = 0; index < partNames.size(); index++) {
        indexMap.put(partNames.get(index), index);
      }
      Map<String, Double> adjustedIndexMap = new HashMap<String, Double>();
      Map<String, ColumnStatisticsData> adjustedStatsMap = new HashMap<String, ColumnStatisticsData>();
      // while we scan the css, we also get the densityAvg, lowerbound and
      // higerbound when useDensityFunctionForNDVEstimation is true.
      double densityAvgSum = 0.0;
      if (ndvEstimator == null) {
        // if not every partition uses bitvector for ndv, we just fall back to
        // the traditional extrapolation methods.
        for (ColumnStatistics cs : css) {
          String partName = cs.getStatsDesc().getPartName();
          ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
          LongColumnStatsData newData = cso.getStatsData().getLongStats();
          if (useDensityFunctionForNDVEstimation) {
            densityAvgSum += (newData.getHighValue() - newData.getLowValue()) / newData.getNumDVs();
          }
          adjustedIndexMap.put(partName, (double) indexMap.get(partName));
          adjustedStatsMap.put(partName, cso.getStatsData());
        }
      } else {
        // we first merge all the adjacent bitvectors that we could merge and
        // derive new partition names and index.
        StringBuilder pseudoPartName = new StringBuilder();
        double pseudoIndexSum = 0;
        int length = 0;
        int curIndex = -1;
        LongColumnStatsData aggregateData = null;
        for (ColumnStatistics cs : css) {
          String partName = cs.getStatsDesc().getPartName();
          ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
          LongColumnStatsData newData = cso.getStatsData().getLongStats();
          // newData.isSetBitVectors() should be true for sure because we
          // already checked it before.
          if (indexMap.get(partName) != curIndex) {
            // There is bitvector, but it is not adjacent to the previous ones.
            if (length > 0) {
              // we have to set ndv
              adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
              aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
              ColumnStatisticsData csd = new ColumnStatisticsData();
              csd.setLongStats(aggregateData);
              adjustedStatsMap.put(pseudoPartName.toString(), csd);
              if (useDensityFunctionForNDVEstimation) {
                densityAvgSum += (aggregateData.getHighValue() - aggregateData.getLowValue()) / aggregateData.getNumDVs();
              }
              // reset everything
              pseudoPartName = new StringBuilder();
              pseudoIndexSum = 0;
              length = 0;
              ndvEstimator = NumDistinctValueEstimatorFactory.getEmptyNumDistinctValueEstimator(ndvEstimator);
            }
            aggregateData = null;
          }
          curIndex = indexMap.get(partName);
          pseudoPartName.append(partName);
          pseudoIndexSum += curIndex;
          length++;
          curIndex++;
          if (aggregateData == null) {
            aggregateData = newData.deepCopy();
          } else {
            aggregateData.setLowValue(Math.min(aggregateData.getLowValue(), newData.getLowValue()));
            aggregateData.setHighValue(Math.max(aggregateData.getHighValue(),
                newData.getHighValue()));
            aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
          }
          ndvEstimator.mergeEstimators(NumDistinctValueEstimatorFactory
              .getNumDistinctValueEstimator(newData.getBitVectors()));
        }
        if (length > 0) {
          // we have to set ndv
          adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
          aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
          ColumnStatisticsData csd = new ColumnStatisticsData();
          csd.setLongStats(aggregateData);
          adjustedStatsMap.put(pseudoPartName.toString(), csd);
          if (useDensityFunctionForNDVEstimation) {
            densityAvgSum += (aggregateData.getHighValue() - aggregateData.getLowValue()) / aggregateData.getNumDVs();
          }
        }
      }
      extrapolate(columnStatisticsData, partNames.size(), css.size(), adjustedIndexMap,
          adjustedStatsMap, densityAvgSum / adjustedStatsMap.size());
    }
    statsObj.setStatsData(columnStatisticsData);
    LOG.debug("Ndv estimatation for " + colName + " is "
        + columnStatisticsData.getLongStats().getNumDVs());
    return statsObj;
  }

  @Override
  public void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg) {
    int rightBorderInd = numParts;
    LongColumnStatsData extrapolateLongData = new LongColumnStatsData();
    Map<String, LongColumnStatsData> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStatisticsData> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), entry.getValue().getLongStats());
    }
    List<Map.Entry<String, LongColumnStatsData>> list = new LinkedList<Map.Entry<String, LongColumnStatsData>>(
        extractedAdjustedStatsMap.entrySet());
    // get the lowValue
    Collections.sort(list, new Comparator<Map.Entry<String, LongColumnStatsData>>() {
      public int compare(Map.Entry<String, LongColumnStatsData> o1,
          Map.Entry<String, LongColumnStatsData> o2) {
        return o1.getValue().getLowValue() < o2.getValue().getLowValue() ? -1 : 1;
      }
    });
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    long lowValue = 0;
    long min = list.get(0).getValue().getLowValue();
    long max = list.get(list.size() - 1).getValue().getLowValue();
    if (minInd == maxInd) {
      lowValue = min;
    } else if (minInd < maxInd) {
      // left border is the min
      lowValue = (long) (max - (max - min) * maxInd / (maxInd - minInd));
    } else {
      // right border is the min
      lowValue = (long) (max - (max - min) * (rightBorderInd - maxInd) / (minInd - maxInd));
    }

    // get the highValue
    Collections.sort(list, new Comparator<Map.Entry<String, LongColumnStatsData>>() {
      public int compare(Map.Entry<String, LongColumnStatsData> o1,
          Map.Entry<String, LongColumnStatsData> o2) {
        return o1.getValue().getHighValue() < o2.getValue().getHighValue() ? -1 : 1;
      }
    });
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    long highValue = 0;
    min = list.get(0).getValue().getHighValue();
    max = list.get(list.size() - 1).getValue().getHighValue();
    if (minInd == maxInd) {
      highValue = min;
    } else if (minInd < maxInd) {
      // right border is the max
      highValue = (long) (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      highValue = (long) (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, LongColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv = 0;
    Collections.sort(list, new Comparator<Map.Entry<String, LongColumnStatsData>>() {
      public int compare(Map.Entry<String, LongColumnStatsData> o1,
          Map.Entry<String, LongColumnStatsData> o2) {
        return o1.getValue().getNumDVs() < o2.getValue().getNumDVs() ? -1 : 1;
      }
    });
    long lowerBound = list.get(list.size() - 1).getValue().getNumDVs();
    long higherBound = 0;
    for (Map.Entry<String, LongColumnStatsData> entry : list) {
      higherBound += entry.getValue().getNumDVs();
    }
    if (useDensityFunctionForNDVEstimation && densityAvg != 0.0) {
      ndv = (long) ((highValue - lowValue) / densityAvg);
      if (ndv < lowerBound) {
        ndv = lowerBound;
      } else if (ndv > higherBound) {
        ndv = higherBound;
      }
    } else {
      minInd = adjustedIndexMap.get(list.get(0).getKey());
      maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
      min = list.get(0).getValue().getNumDVs();
      max = list.get(list.size() - 1).getValue().getNumDVs();
      if (minInd == maxInd) {
        ndv = min;
      } else if (minInd < maxInd) {
        // right border is the max
        ndv = (long) (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
      } else {
        // left border is the max
        ndv = (long) (min + (max - min) * minInd / (minInd - maxInd));
      }
    }
    extrapolateLongData.setLowValue(lowValue);
    extrapolateLongData.setHighValue(highValue);
    extrapolateLongData.setNumNulls(numNulls);
    extrapolateLongData.setNumDVs(ndv);
    extrapolateData.setLongStats(extrapolateLongData);
  }

}
