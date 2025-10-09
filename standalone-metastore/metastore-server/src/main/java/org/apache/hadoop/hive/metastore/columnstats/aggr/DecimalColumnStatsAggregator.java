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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.merge.DecimalColumnStatsMerger;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.decimalInspectorFromStats;

public class DecimalColumnStatsAggregator extends ColumnStatsAggregator implements
    IExtrapolatePartStatus {

  private static final Logger LOG = LoggerFactory.getLogger(DecimalColumnStatsAggregator.class);

  @Override
  public ColumnStatisticsObj aggregate(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo,
      List<String> partNames, boolean areAllPartsFound) throws MetaException {
    checkStatisticsList(colStatsWithSourceInfo);

    ColumnStatisticsObj statsObj = null;
    String colType;
    String colName = null;
    // check if all the ColumnStatisticsObjs contain stats and all the ndv are
    // bitvectors
    boolean doAllPartitionContainStats = partNames.size() == colStatsWithSourceInfo.size();
    NumDistinctValueEstimator ndvEstimator = null;
    boolean areAllNDVEstimatorsMergeable = true;
    for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
      ColumnStatisticsObj cso = csp.getColStatsObj();
      if (statsObj == null) {
        colName = cso.getColName();
        colType = cso.getColType();
        statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType,
            cso.getStatsData().getSetField());
        LOG.trace("doAllPartitionContainStats for column: {} is: {}", colName,
            doAllPartitionContainStats);
      }
      DecimalColumnStatsDataInspector columnStatsData = decimalInspectorFromStats(cso);

      // check if we can merge NDV estimators
      if (columnStatsData.getNdvEstimator() == null) {
        areAllNDVEstimatorsMergeable = false;
        break;
      } else {
        NumDistinctValueEstimator estimator = columnStatsData.getNdvEstimator();
        if (ndvEstimator == null) {
          ndvEstimator = estimator;
        } else {
          if (!ndvEstimator.canMerge(estimator)) {
            areAllNDVEstimatorsMergeable = false;
            break;
          }
        }
      }
    }
    if (areAllNDVEstimatorsMergeable && ndvEstimator != null) {
      ndvEstimator = NumDistinctValueEstimatorFactory.getEmptyNumDistinctValueEstimator(ndvEstimator);
    }
    LOG.debug("all of the bit vectors can merge for {} is {}", colName, areAllNDVEstimatorsMergeable);

    ColumnStatisticsData columnStatisticsData = initColumnStatisticsData();
    if (doAllPartitionContainStats || colStatsWithSourceInfo.size() < 2) {
      DecimalColumnStatsDataInspector aggregateData = null;
      long lowerBound = 0;
      long higherBound = 0;
      double densityAvgSum = 0.0;
      DecimalColumnStatsMerger merger = new DecimalColumnStatsMerger();
      for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
        ColumnStatisticsObj cso = csp.getColStatsObj();
        DecimalColumnStatsDataInspector newData = decimalInspectorFromStats(cso);
        lowerBound = Math.max(lowerBound, newData.getNumDVs());
        higherBound += newData.getNumDVs();
        if (newData.isSetLowValue() && newData.isSetHighValue()) {
          densityAvgSum += (MetaStoreServerUtils.decimalToDouble(newData.getHighValue()) - MetaStoreServerUtils
              .decimalToDouble(newData.getLowValue())) / newData.getNumDVs();
        }
        if (areAllNDVEstimatorsMergeable && ndvEstimator != null) {
          ndvEstimator.mergeEstimators(newData.getNdvEstimator());
        }
        if (aggregateData == null) {
          aggregateData = newData.deepCopy();
        } else {
          aggregateData.setLowValue(merger.mergeLowValue(
              merger.getLowValue(aggregateData), merger.getLowValue(newData)));
          aggregateData.setHighValue(merger.mergeHighValue(
              merger.getHighValue(aggregateData), merger.getHighValue(newData)));

          aggregateData.setNumNulls(merger.mergeNumNulls(aggregateData.getNumNulls(), newData.getNumNulls()));
          aggregateData.setNumDVs(merger.mergeNumDVs(aggregateData.getNumDVs(), newData.getNumDVs()));
        }
      }
      if (areAllNDVEstimatorsMergeable && ndvEstimator != null) {
        // if all the ColumnStatisticsObjs contain bitvectors, we do not need to
        // use uniform distribution assumption because we can merge bitvectors
        // to get a good estimation.
        aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
      } else {
        long estimation;
        if (useDensityFunctionForNDVEstimation && aggregateData != null
            && aggregateData.isSetLowValue() && aggregateData.isSetHighValue()) {
          // We have estimation, lowerbound and higherbound. We use estimation
          // if it is between lowerbound and higherbound.
          double densityAvg = densityAvgSum / partNames.size();
          estimation = (long) ((MetaStoreServerUtils.decimalToDouble(aggregateData.getHighValue()) - MetaStoreServerUtils
              .decimalToDouble(aggregateData.getLowValue())) / densityAvg);
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

      columnStatisticsData.setDecimalStats(aggregateData);
    } else {
      // TODO: bail out if missing stats are over a certain threshold
      // we need extrapolation
      LOG.debug("start extrapolation for {}", colName);
      Map<String, Integer> indexMap = new HashMap<>();
      for (int index = 0; index < partNames.size(); index++) {
        indexMap.put(partNames.get(index), index);
      }
      Map<String, Double> adjustedIndexMap = new HashMap<>();
      Map<String, ColumnStatisticsData> adjustedStatsMap = new HashMap<>();
      // while we scan the css, we also get the densityAvg, lowerbound and
      // higherbound when useDensityFunctionForNDVEstimation is true.
      double densityAvgSum = 0.0;
      if (!areAllNDVEstimatorsMergeable) {
        // if not every partition uses bitvector for ndv, we just fall back to
        // the traditional extrapolation methods.
        for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
          ColumnStatisticsObj cso = csp.getColStatsObj();
          String partName = csp.getPartName();
          DecimalColumnStatsData newData = cso.getStatsData().getDecimalStats();
          if (useDensityFunctionForNDVEstimation && newData.isSetLowValue() && newData.isSetHighValue()) {
            densityAvgSum += (MetaStoreServerUtils.decimalToDouble(newData.getHighValue()) - MetaStoreServerUtils
                .decimalToDouble(newData.getLowValue())) / newData.getNumDVs();
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
        DecimalColumnStatsDataInspector aggregateData = null;
        for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
          ColumnStatisticsObj cso = csp.getColStatsObj();
          String partName = csp.getPartName();
          DecimalColumnStatsDataInspector newData = decimalInspectorFromStats(cso);
          // newData.isSetBitVectors() should be true for sure because we
          // already checked it before.
          if (indexMap.get(partName) != curIndex) {
            // There is bitvector, but it is not adjacent to the previous ones.
            if (length > 0) {
              // we have to set ndv
              adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
              aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
              ColumnStatisticsData csd = new ColumnStatisticsData();
              csd.setDecimalStats(aggregateData);
              adjustedStatsMap.put(pseudoPartName.toString(), csd);
              if (useDensityFunctionForNDVEstimation) {
                densityAvgSum += (MetaStoreServerUtils.decimalToDouble(aggregateData.getHighValue()) - MetaStoreServerUtils
                    .decimalToDouble(aggregateData.getLowValue())) / aggregateData.getNumDVs();
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
            if (MetaStoreServerUtils.decimalToDouble(aggregateData.getLowValue()) < MetaStoreServerUtils
                .decimalToDouble(newData.getLowValue())) {
              aggregateData.setLowValue(aggregateData.getLowValue());
            } else {
              aggregateData.setLowValue(newData.getLowValue());
            }
            if (MetaStoreServerUtils.decimalToDouble(aggregateData.getHighValue()) > MetaStoreServerUtils
                .decimalToDouble(newData.getHighValue())) {
              aggregateData.setHighValue(aggregateData.getHighValue());
            } else {
              aggregateData.setHighValue(newData.getHighValue());
            }
            aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
          }
          ndvEstimator.mergeEstimators(newData.getNdvEstimator());
        }
        if (length > 0) {
          // we have to set ndv
          adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
          aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
          ColumnStatisticsData csd = new ColumnStatisticsData();
          csd.setDecimalStats(aggregateData);
          adjustedStatsMap.put(pseudoPartName.toString(), csd);
          if (useDensityFunctionForNDVEstimation) {
            densityAvgSum += (MetaStoreServerUtils.decimalToDouble(aggregateData.getHighValue()) - MetaStoreServerUtils
                .decimalToDouble(aggregateData.getLowValue())) / aggregateData.getNumDVs();
          }
        }
      }
      extrapolate(columnStatisticsData, partNames.size(), colStatsWithSourceInfo.size(),
          adjustedIndexMap, adjustedStatsMap, densityAvgSum / adjustedStatsMap.size());
    }
    LOG.debug(
        "Ndv estimation for {} is {}. # of partitions requested: {}. # of partitions found: {}",
        colName, columnStatisticsData.getDecimalStats().getNumDVs(), partNames.size(),
        colStatsWithSourceInfo.size());

    KllHistogramEstimator mergedKllHistogramEstimator = mergeHistograms(colStatsWithSourceInfo);
    if (mergedKllHistogramEstimator != null) {
      columnStatisticsData.getDecimalStats().setHistogram(mergedKllHistogramEstimator.serialize());
    }

    statsObj.setStatsData(columnStatisticsData);
    return statsObj;
  }

  @Override protected ColumnStatisticsData initColumnStatisticsData() {
    ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
    // init stats internal data if missing, re-use if existing
    if (!columnStatisticsData.isSetDecimalStats()) {
      columnStatisticsData.setDecimalStats(new DecimalColumnStatsData());
    }
    return columnStatisticsData;
  }

  @Override
  public void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg) {

    Map<String, DecimalColumnStatsData> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStatisticsData> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), entry.getValue().getDecimalStats());
    }
    List<Map.Entry<String, DecimalColumnStatsData>> list = new LinkedList<>(
        extractedAdjustedStatsMap.entrySet());
    // get the lowValue
    list.sort(Comparator.comparing(o -> o.getValue().getLowValue()));
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double lowValue;
    double min = MetaStoreServerUtils.decimalToDouble(list.get(0).getValue().getLowValue());
    double max = MetaStoreServerUtils.decimalToDouble(list.get(list.size() - 1).getValue().getLowValue());
    if (minInd == maxInd) {
      lowValue = min;
    } else if (minInd < maxInd) {
      // left border is the min
      lowValue = (max - (max - min) * maxInd / (maxInd - minInd));
    } else {
      // right border is the min
      lowValue = (max - (max - min) * (numParts - maxInd) / (minInd - maxInd));
    }

    // get the highValue
    list.sort(Comparator.comparing(o -> o.getValue().getHighValue()));
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double highValue;
    min = MetaStoreServerUtils.decimalToDouble(list.get(0).getValue().getHighValue());
    max = MetaStoreServerUtils.decimalToDouble(list.get(list.size() - 1).getValue().getHighValue());
    if (minInd == maxInd) {
      highValue = min;
    } else if (minInd < maxInd) {
      // right border is the max
      highValue = (min + (max - min) * (numParts - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      highValue = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, DecimalColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv;
    long ndvMin;
    long ndvMax;
    list.sort(Comparator.comparingLong(o -> o.getValue().getNumDVs()));
    long lowerBound = list.get(list.size() - 1).getValue().getNumDVs();
    long higherBound = 0;
    for (Map.Entry<String, DecimalColumnStatsData> entry : list) {
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
      ndvMin = list.get(0).getValue().getNumDVs();
      ndvMax = list.get(list.size() - 1).getValue().getNumDVs();
      if (minInd == maxInd) {
        ndv = ndvMin;
      } else if (minInd < maxInd) {
        // right border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * (numParts - minInd) / (maxInd - minInd));
      } else {
        // left border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * minInd / (minInd - maxInd));
      }
    }
    DecimalColumnStatsData extrapolateDecimalData = extrapolateData.getDecimalStats();
    extrapolateDecimalData.setLowValue(DecimalUtils.createThriftDecimal(String.valueOf(lowValue)));
    extrapolateDecimalData.setHighValue(DecimalUtils.createThriftDecimal(String.valueOf(highValue)));
    extrapolateDecimalData.setNumNulls(numNulls);
    extrapolateDecimalData.setNumDVs(ndv);
  }
}
