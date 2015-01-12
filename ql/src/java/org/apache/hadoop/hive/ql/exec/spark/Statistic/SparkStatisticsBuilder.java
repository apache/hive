/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.Statistic;

import org.apache.hive.spark.counter.SparkCounter;
import org.apache.hive.spark.counter.SparkCounterGroup;
import org.apache.hive.spark.counter.SparkCounters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SparkStatisticsBuilder {

  private Map<String, List<SparkStatistic>> statisticMap;

  public SparkStatisticsBuilder() {
    statisticMap = new HashMap<String, List<SparkStatistic>>();
  }

  public SparkStatistics build() {
    List<SparkStatisticGroup> statisticGroups = new LinkedList<SparkStatisticGroup>();
    for (Map.Entry<String, List<SparkStatistic>> entry : statisticMap.entrySet()) {
      String groupName = entry.getKey();
      List<SparkStatistic> statisitcList = entry.getValue();
      statisticGroups.add(new SparkStatisticGroup(groupName, statisitcList));
    }

    return new SparkStatistics(statisticGroups);
  }

  public SparkStatisticsBuilder add(SparkCounters sparkCounters) {
    for (SparkCounterGroup counterGroup : sparkCounters.getSparkCounterGroups().values()) {
      String groupDisplayName = counterGroup.getGroupDisplayName();
      List<SparkStatistic> statisticList = statisticMap.get(groupDisplayName);
      if (statisticList == null) {
        statisticList = new LinkedList<SparkStatistic>();
        statisticMap.put(groupDisplayName, statisticList);
      }
      for (SparkCounter counter : counterGroup.getSparkCounters().values()) {
        String displayName = counter.getDisplayName();
        statisticList.add(new SparkStatistic(displayName, Long.toString(counter.getValue())));
      }
    }
    return this;
  }

  public SparkStatisticsBuilder add(String groupName, String name, String value) {
    List<SparkStatistic> statisticList = statisticMap.get(groupName);
    if (statisticList == null) {
      statisticList = new LinkedList<SparkStatistic>();
      statisticMap.put(groupName, statisticList);
    }
    statisticList.add(new SparkStatistic(name, value));
    return this;
  }
}
