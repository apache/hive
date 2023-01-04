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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_STATS_TOP_EVENTS_COUNTS;

/**
 * Tracks the replication statistics per event type.
 */
public class ReplStatsTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ReplStatsTracker.class);
  // Maintains the length of the RM_Progress column in the RDBMS, which stores the ReplStats
  public static int RM_PROGRESS_LENGTH = 10000;
  public static int TOP_K_MAX = 10;

  // Maintains the descriptive statistics per event type.
  private ConcurrentHashMap<String, DescriptiveStatistics> descMap;

  // Maintains the top K costliest eventId's
  private ConcurrentHashMap<String, ListOrderedMap<Long, Long>> topKEvents;
  // Number of top events to maintain.
  private final int k;

  private String lastEventId;

  public ReplStatsTracker(int k) {
    if (k > TOP_K_MAX) {
      LOG.warn("Value for {} exceeded maximum permissible limit. Using Maximum of {}", REPL_STATS_TOP_EVENTS_COUNTS,
              TOP_K_MAX);
      k = TOP_K_MAX;
    }
    this.k = k;
    descMap = new ConcurrentHashMap<>();
    topKEvents = new ConcurrentHashMap<>();
  }

  /**
   * Adds an entry for tracking.
   * @param eventType the type of event.
   * @param eventId the event id.
   * @param timeTaken time taken to process the event.
   */
  public synchronized void addEntry(String eventType, String eventId, long timeTaken) {
    // Store the last EventId for the JMX.
    lastEventId = eventId;
    // Update the entry in the descriptive statistics.
    DescriptiveStatistics descStatistics = descMap.get(eventType);
    if (descStatistics == null) {
      descStatistics = new DescriptiveStatistics();
      descStatistics.addValue(timeTaken);
      descMap.put(eventType, descStatistics);
    } else {
      descStatistics.addValue(timeTaken);
    }

    // Tracking for top K events, Maintain the list in descending order.
    ListOrderedMap<Long, Long> topKEntries = topKEvents.get(eventType);
    if (topKEntries == null) {
      topKEntries = new ListOrderedMap<>();
      topKEntries.put(Long.parseLong(eventId), timeTaken);
      topKEvents.put(eventType, topKEntries);
    } else {
      // Get the index of insertion, by descending order.
      int index = Collections.binarySearch(new ArrayList(topKEntries.values()), timeTaken, Collections.reverseOrder());
      // If the element comes as top K add it to the topEntries.
      // The index returned from the binary search, is either the index where the element already exist, else
      // (-insertionIndex) -1, so convert it to actual insertion index
      int insertionIndex = index < 0 ? -1 * (index) - 1 : index;
      if (insertionIndex < k && k >= 0) {
        topKEntries.put(insertionIndex, Long.parseLong(eventId), timeTaken);
      }
      // Post addition, if the size moves up by K, then remove the last entry, that would be smallest of the K
      // entries, since the list is sorted in descending order.
      if (topKEntries.size() > k) {
        topKEntries.remove(k);
      }
    }
  }

  /**
   * Get the DescriptiveStatistics for each event type.
   * @return A HashMap, with key as event type &amp; value as the DescriptiveAnalytics of the entire run.
   */
  public ConcurrentHashMap<String, DescriptiveStatistics> getDescMap() {
    return descMap;
  }

  /**
   * Gets the top K costliest event per event type.
   * @return A HashMap with key as the event type and a Map as values, which has the event id as key and time taken
   * as values.
   */
  public ConcurrentHashMap<String, ListOrderedMap<Long, Long>> getTopKEvents() {
    return topKEvents;
  }

  /**
   * Gets the last event id processed.
   * @return the last event id.
   */
  public String getLastEventId() {
    return lastEventId;
  }

  public int getK() {
    return k;
  }

  private String formatDouble(DecimalFormat dFormat, Double d) {
    if (!d.isNaN()) {
      return dFormat.format(d);
    }
    return d.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    DecimalFormat dFormat = new DecimalFormat("#.##");
    dFormat.setRoundingMode(RoundingMode.HALF_UP);
    sb.append("Replication Stats{");
    for (Map.Entry<String, DescriptiveStatistics> event : descMap.entrySet()) {
      DescriptiveStatistics statistics = event.getValue();
      sb.append("[[Event Name: ").append(event.getKey()).append("; ");
      sb.append("Total Number: ").append(statistics.getN()).append("; ");
      sb.append("Total Time: ").append(dFormat.format(statistics.getSum())).append("; ");
      sb.append("Mean: ").append(formatDouble(dFormat, statistics.getMean())).append("; ");
      sb.append("Median: ").append(formatDouble(dFormat, statistics.getPercentile(50))).append("; ");
      sb.append("Standard Deviation: ").append(formatDouble(dFormat, statistics.getStandardDeviation())).append("; ");
      sb.append("Variance: ").append(formatDouble(dFormat, statistics.getVariance())).append("; ");
      sb.append("Kurtosis: ").append(formatDouble(dFormat, statistics.getKurtosis())).append("; ");
      sb.append("Skewness: ").append(formatDouble(dFormat, statistics.getSkewness())).append("; ");
      sb.append("25th Percentile: ").append(formatDouble(dFormat, statistics.getPercentile(25))).append("; ");
      sb.append("50th Percentile: ").append(formatDouble(dFormat, statistics.getPercentile(50))).append("; ");
      sb.append("75th Percentile: ").append(formatDouble(dFormat, statistics.getPercentile(75))).append("; ");
      sb.append("90th Percentile: ").append(formatDouble(dFormat, statistics.getPercentile(90))).append("; ");
      sb.append("Top ").append(k).append(" EventIds(EventId=Time) ").append(topKEvents.get(event.getKey()))
          .append(";" + "]]");
    }
    sb.append("}");
    return sb.toString();
  }
}


