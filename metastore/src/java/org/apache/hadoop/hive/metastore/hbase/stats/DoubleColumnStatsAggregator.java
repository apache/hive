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

package org.apache.hadoop.hive.metastore.hbase.stats;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class DoubleColumnStatsAggregator extends ColumnStatsAggregator implements
    IExtrapolatePartStatus {

  @Override
  public ColumnStatisticsObj aggregate(String colName, List<String> partNames,
      List<ColumnStatistics> css) throws MetaException {
    ColumnStatisticsObj statsObj = null;

    // check if all the ColumnStatisticsObjs contain stats and all the ndv are
    // bitvectors
    boolean doAllPartitionContainStats = partNames.size() == css.size();
    boolean isNDVBitVectorSet = true;
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
      if (numBitVectors <= 0 || !cso.getStatsData().getDoubleStats().isSetBitVectors()
          || cso.getStatsData().getDoubleStats().getBitVectors().length() == 0) {
        isNDVBitVectorSet = false;
        break;
      }
    }
    ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
    if (doAllPartitionContainStats || css.size() < 2) {
      DoubleColumnStatsData aggregateData = null;
      long lowerBound = 0;
      long higherBound = 0;
      double densityAvgSum = 0.0;
      NumDistinctValueEstimator ndvEstimator = null;
      if (isNDVBitVectorSet) {
        ndvEstimator = new NumDistinctValueEstimator(numBitVectors);
      }
      for (ColumnStatistics cs : css) {
        ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
        DoubleColumnStatsData newData = cso.getStatsData().getDoubleStats();
        if (useDensityFunctionForNDVEstimation) {
          lowerBound = Math.max(lowerBound, newData.getNumDVs());
          higherBound += newData.getNumDVs();
          densityAvgSum += (newData.getHighValue() - newData.getLowValue()) / newData.getNumDVs();
        }
        if (isNDVBitVectorSet) {
          ndvEstimator.mergeEstimators(new NumDistinctValueEstimator(newData.getBitVectors(),
              ndvEstimator.getnumBitVectors()));
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
      if (isNDVBitVectorSet) {
        // if all the ColumnStatisticsObjs contain bitvectors, we do not need to
        // use uniform distribution assumption because we can merge bitvectors
        // to get a good estimation.
        aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
      } else {
        if (useDensityFunctionForNDVEstimation) {
          // We have estimation, lowerbound and higherbound. We use estimation
          // if it is between lowerbound and higherbound.
          double densityAvg = densityAvgSum / partNames.size();
          long estimation = (long) ((aggregateData.getHighValue() - aggregateData.getLowValue()) / densityAvg);
          if (estimation < lowerBound) {
            aggregateData.setNumDVs(lowerBound);
          } else if (estimation > higherBound) {
            aggregateData.setNumDVs(higherBound);
          } else {
            aggregateData.setNumDVs(estimation);
          }
        } else {
          // Without useDensityFunctionForNDVEstimation, we just use the
          // default one, which is the max of all the partitions and it is
          // already done.
        }
      }
      columnStatisticsData.setDoubleStats(aggregateData);
    } else {
      // we need extrapolation
      Map<String, Integer> indexMap = new HashMap<String, Integer>();
      for (int index = 0; index < partNames.size(); index++) {
        indexMap.put(partNames.get(index), index);
      }
      Map<String, Double> adjustedIndexMap = new HashMap<String, Double>();
      Map<String, ColumnStatisticsData> adjustedStatsMap = new HashMap<String, ColumnStatisticsData>();
      // while we scan the css, we also get the densityAvg, lowerbound and
      // higerbound when useDensityFunctionForNDVEstimation is true.
      double densityAvgSum = 0.0;
      if (!isNDVBitVectorSet) {
        // if not every partition uses bitvector for ndv, we just fall back to
        // the traditional extrapolation methods.
        for (ColumnStatistics cs : css) {
          String partName = cs.getStatsDesc().getPartName();
          ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
          DoubleColumnStatsData newData = cso.getStatsData().getDoubleStats();
          if (useDensityFunctionForNDVEstimation) {
            densityAvgSum += (newData.getHighValue() - newData.getLowValue()) / newData.getNumDVs();
          }
          adjustedIndexMap.put(partName, (double) indexMap.get(partName));
          adjustedStatsMap.put(partName, cso.getStatsData());
        }
      } else {
        // we first merge all the adjacent bitvectors that we could merge and
        // derive new partition names and index.
        NumDistinctValueEstimator ndvEstimator = new NumDistinctValueEstimator(numBitVectors);
        StringBuilder pseudoPartName = new StringBuilder();
        double pseudoIndexSum = 0;
        int length = 0;
        int curIndex = -1;
        DoubleColumnStatsData aggregateData = null;
        for (ColumnStatistics cs : css) {
          String partName = cs.getStatsDesc().getPartName();
          ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
          DoubleColumnStatsData newData = cso.getStatsData().getDoubleStats();
          // newData.isSetBitVectors() should be true for sure because we
          // already checked it before.
          if (indexMap.get(partName) != curIndex) {
            // There is bitvector, but it is not adjacent to the previous ones.
            if (length > 0) {
              // we have to set ndv
              adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
              aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
              ColumnStatisticsData csd = new ColumnStatisticsData();
              csd.setDoubleStats(aggregateData);
              adjustedStatsMap.put(pseudoPartName.toString(), csd);
              if (useDensityFunctionForNDVEstimation) {
                densityAvgSum += (aggregateData.getHighValue() - aggregateData.getLowValue()) / aggregateData.getNumDVs();
              }
              // reset everything
              pseudoPartName = new StringBuilder();
              pseudoIndexSum = 0;
              length = 0;
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
          ndvEstimator.mergeEstimators(new NumDistinctValueEstimator(newData.getBitVectors(),
              ndvEstimator.getnumBitVectors()));
        }
        if (length > 0) {
          // we have to set ndv
          adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
          aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
          ColumnStatisticsData csd = new ColumnStatisticsData();
          csd.setDoubleStats(aggregateData);
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
    return statsObj;
  }

  @Override
  public void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg) {
    int rightBorderInd = numParts;
    DoubleColumnStatsData extrapolateDoubleData = new DoubleColumnStatsData();
    Map<String, DoubleColumnStatsData> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStatisticsData> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), entry.getValue().getDoubleStats());
    }
    List<Map.Entry<String, DoubleColumnStatsData>> list = new LinkedList<Map.Entry<String, DoubleColumnStatsData>>(
        extractedAdjustedStatsMap.entrySet());
    // get the lowValue
    Collections.sort(list, new Comparator<Map.Entry<String, DoubleColumnStatsData>>() {
      public int compare(Map.Entry<String, DoubleColumnStatsData> o1,
          Map.Entry<String, DoubleColumnStatsData> o2) {
        return o1.getValue().getLowValue() < o2.getValue().getLowValue() ? -1 : 1;
      }
    });
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double lowValue = 0;
    double min = list.get(0).getValue().getLowValue();
    double max = list.get(list.size() - 1).getValue().getLowValue();
    if (minInd == maxInd) {
      lowValue = min;
    } else if (minInd < maxInd) {
      // left border is the min
      lowValue = (max - (max - min) * maxInd / (maxInd - minInd));
    } else {
      // right border is the min
      lowValue = (max - (max - min) * (rightBorderInd - maxInd) / (minInd - maxInd));
    }

    // get the highValue
    Collections.sort(list, new Comparator<Map.Entry<String, DoubleColumnStatsData>>() {
      public int compare(Map.Entry<String, DoubleColumnStatsData> o1,
          Map.Entry<String, DoubleColumnStatsData> o2) {
        return o1.getValue().getHighValue() < o2.getValue().getHighValue() ? -1 : 1;
      }
    });
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double highValue = 0;
    min = list.get(0).getValue().getHighValue();
    max = list.get(list.size() - 1).getValue().getHighValue();
    if (minInd == maxInd) {
      highValue = min;
    } else if (minInd < maxInd) {
      // right border is the max
      highValue = (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      highValue = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, DoubleColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv = 0;
    long ndvMin = 0;
    long ndvMax = 0;
    Collections.sort(list, new Comparator<Map.Entry<String, DoubleColumnStatsData>>() {
      public int compare(Map.Entry<String, DoubleColumnStatsData> o1,
          Map.Entry<String, DoubleColumnStatsData> o2) {
        return o1.getValue().getNumDVs() < o2.getValue().getNumDVs() ? -1 : 1;
      }
    });
    long lowerBound = list.get(list.size() - 1).getValue().getNumDVs();
    long higherBound = 0;
    for (Map.Entry<String, DoubleColumnStatsData> entry : list) {
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
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * (rightBorderInd - minInd) / (maxInd - minInd));
      } else {
        // left border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * minInd / (minInd - maxInd));
      }
    }
    extrapolateDoubleData.setLowValue(lowValue);
    extrapolateDoubleData.setHighValue(highValue);
    extrapolateDoubleData.setNumNulls(numNulls);
    extrapolateDoubleData.setNumDVs(ndv);
    extrapolateData.setDoubleStats(extrapolateDoubleData);
  }
  
}
