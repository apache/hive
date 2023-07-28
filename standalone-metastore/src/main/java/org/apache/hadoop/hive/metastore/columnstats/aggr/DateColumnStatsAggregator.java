/*
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
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.dateInspectorFromStats;

public class DateColumnStatsAggregator extends ColumnStatsAggregator implements
    IExtrapolatePartStatus {

  private static final Logger LOG = LoggerFactory.getLogger(DateColumnStatsAggregator.class);

  @Override
  public ColumnStatisticsObj aggregate(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo,
            List<String> partNames, boolean areAllPartsFound) throws MetaException {
    ColumnStatisticsObj statsObj = null;
    String colType = null;
    String colName = null;
    // check if all the ColumnStatisticsObjs contain stats and all the ndv are
    // bitvectors
    boolean doAllPartitionContainStats = partNames.size() == colStatsWithSourceInfo.size();
    NumDistinctValueEstimator ndvEstimator = null;
    for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
      ColumnStatisticsObj cso = csp.getColStatsObj();
      if (statsObj == null) {
        colName = cso.getColName();
        colType = cso.getColType();
        statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType,
            cso.getStatsData().getSetField());
        LOG.trace("doAllPartitionContainStats for column: {} is: {}", colName, doAllPartitionContainStats);
      }
      DateColumnStatsDataInspector dateColumnStats = dateInspectorFromStats(cso);

      if (dateColumnStats.getNdvEstimator() == null) {
        ndvEstimator = null;
        break;
      } else {
        // check if all of the bit vectors can merge
        NumDistinctValueEstimator estimator = dateColumnStats.getNdvEstimator();
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
    if (doAllPartitionContainStats || colStatsWithSourceInfo.size() < 2) {
      DateColumnStatsDataInspector aggregateData = null;
      long lowerBound = 0;
      long higherBound = 0;
      double densityAvgSum = 0.0;
      for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
        ColumnStatisticsObj cso = csp.getColStatsObj();
        DateColumnStatsDataInspector newData = dateInspectorFromStats(cso);
        higherBound += newData.getNumDVs();
        densityAvgSum += (diff(newData.getHighValue(), newData.getLowValue()))
            / newData.getNumDVs();
        if (ndvEstimator != null) {
          ndvEstimator.mergeEstimators(newData.getNdvEstimator());
        }
        if (aggregateData == null) {
          aggregateData = newData.deepCopy();
        } else {
          aggregateData.setLowValue(min(aggregateData.getLowValue(), newData.getLowValue()));
          aggregateData
              .setHighValue(max(aggregateData.getHighValue(), newData.getHighValue()));
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
          estimation = (long) (diff(aggregateData.getHighValue(), aggregateData.getLowValue()) / densityAvg);
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
      columnStatisticsData.setDateStats(aggregateData);
    } else {
      // we need extrapolation
      LOG.debug("start extrapolation for " + colName);

      Map<String, Integer> indexMap = new HashMap<>();
      for (int index = 0; index < partNames.size(); index++) {
        indexMap.put(partNames.get(index), index);
      }
      Map<String, Double> adjustedIndexMap = new HashMap<>();
      Map<String, ColumnStatisticsData> adjustedStatsMap = new HashMap<>();
      // while we scan the css, we also get the densityAvg, lowerbound and
      // higerbound when useDensityFunctionForNDVEstimation is true.
      double densityAvgSum = 0.0;
      if (ndvEstimator == null) {
        // if not every partition uses bitvector for ndv, we just fall back to
        // the traditional extrapolation methods.
        for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
          ColumnStatisticsObj cso = csp.getColStatsObj();
          String partName = csp.getPartName();
          DateColumnStatsData newData = cso.getStatsData().getDateStats();
          if (useDensityFunctionForNDVEstimation) {
            densityAvgSum += diff(newData.getHighValue(), newData.getLowValue()) / newData.getNumDVs();
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
        DateColumnStatsDataInspector aggregateData = null;
        for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
          ColumnStatisticsObj cso = csp.getColStatsObj();
          String partName = csp.getPartName();
          DateColumnStatsDataInspector newData = dateInspectorFromStats(cso);
          // newData.isSetBitVectors() should be true for sure because we
          // already checked it before.
          if (indexMap.get(partName) != curIndex) {
            // There is bitvector, but it is not adjacent to the previous ones.
            if (length > 0) {
              // we have to set ndv
              adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
              aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
              ColumnStatisticsData csd = new ColumnStatisticsData();
              csd.setDateStats(aggregateData);
              adjustedStatsMap.put(pseudoPartName.toString(), csd);
              if (useDensityFunctionForNDVEstimation) {
                densityAvgSum += diff(aggregateData.getHighValue(), aggregateData.getLowValue())
                    / aggregateData.getNumDVs();
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
            aggregateData.setLowValue(min(aggregateData.getLowValue(), newData.getLowValue()));
            aggregateData.setHighValue(max(aggregateData.getHighValue(), newData.getHighValue()));
            aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
          }
          ndvEstimator.mergeEstimators(newData.getNdvEstimator());
        }
        if (length > 0) {
          // we have to set ndv
          adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
          aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
          ColumnStatisticsData csd = new ColumnStatisticsData();
          csd.setDateStats(aggregateData);
          adjustedStatsMap.put(pseudoPartName.toString(), csd);
          if (useDensityFunctionForNDVEstimation) {
            densityAvgSum += diff(aggregateData.getHighValue(), aggregateData.getLowValue())
                / aggregateData.getNumDVs();
          }
        }
      }
      extrapolate(columnStatisticsData, partNames.size(), colStatsWithSourceInfo.size(),
          adjustedIndexMap, adjustedStatsMap, densityAvgSum / adjustedStatsMap.size());
    }
    LOG.debug(
        "Ndv estimatation for {} is {} # of partitions requested: {} # of partitions found: {}",
        colName, columnStatisticsData.getDateStats().getNumDVs(), partNames.size(),
        colStatsWithSourceInfo.size());
    statsObj.setStatsData(columnStatisticsData);
    return statsObj;
  }

  private long diff(Date d1, Date d2) {
    return d1.getDaysSinceEpoch() - d2.getDaysSinceEpoch();
  }

  private Date min(Date d1, Date d2) {
    return d1.compareTo(d2) < 0 ? d1 : d2;
  }

  private Date max(Date d1, Date d2) {
    return d1.compareTo(d2) < 0 ? d2 : d1;
  }

  @Override
  public void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg) {
    int rightBorderInd = numParts;
    DateColumnStatsDataInspector extrapolateDateData = new DateColumnStatsDataInspector();
    Map<String, DateColumnStatsData> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStatisticsData> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), entry.getValue().getDateStats());
    }
    List<Map.Entry<String, DateColumnStatsData>> list = new LinkedList<>(
        extractedAdjustedStatsMap.entrySet());
    // get the lowValue
    Collections.sort(list, new Comparator<Map.Entry<String, DateColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, DateColumnStatsData> o1,
          Map.Entry<String, DateColumnStatsData> o2) {
        return o1.getValue().getLowValue().compareTo(o2.getValue().getLowValue());
      }
    });
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    long lowValue = 0;
    long min = list.get(0).getValue().getLowValue().getDaysSinceEpoch();
    long max = list.get(list.size() - 1).getValue().getLowValue().getDaysSinceEpoch();
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
    Collections.sort(list, new Comparator<Map.Entry<String, DateColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, DateColumnStatsData> o1,
          Map.Entry<String, DateColumnStatsData> o2) {
        return o1.getValue().getHighValue().compareTo(o2.getValue().getHighValue());
      }
    });
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    long highValue = 0;
    min = list.get(0).getValue().getHighValue().getDaysSinceEpoch();
    max = list.get(list.size() - 1).getValue().getHighValue().getDaysSinceEpoch();
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
    for (Map.Entry<String, DateColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv = 0;
    Collections.sort(list, new Comparator<Map.Entry<String, DateColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, DateColumnStatsData> o1,
          Map.Entry<String, DateColumnStatsData> o2) {
        return Long.compare(o1.getValue().getNumDVs(), o2.getValue().getNumDVs());
      }
    });
    long lowerBound = list.get(list.size() - 1).getValue().getNumDVs();
    long higherBound = 0;
    for (Map.Entry<String, DateColumnStatsData> entry : list) {
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
    extrapolateDateData.setLowValue(new Date(lowValue));
    extrapolateDateData.setHighValue(new Date(highValue));
    extrapolateDateData.setNumNulls(numNulls);
    extrapolateDateData.setNumDVs(ndv);
    extrapolateData.setDateStats(extrapolateDateData);
  }
}
