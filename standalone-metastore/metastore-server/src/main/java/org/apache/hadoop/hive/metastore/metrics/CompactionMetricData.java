/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.metrics;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.NO_VAL;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getHostFromId;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getThreadIdFromId;

final class CompactionMetricData {

  private static final Long OLDEST_TIME_NO_VALUE = Long.MAX_VALUE;

  private final List<ShowCompactResponseElement> compacts;

  private long oldestEnqueueTime;
  private long oldestWorkingTime;
  private long oldestCleaningTime;

  private Map<String, Long> stateCount;

  private Map<String, Integer> poolCount;

  private Double failedCompactionPercentage;

  private long initiatorsCount;
  private long initiatorVersionsCount;
  private long workersCount;
  private long workerVersionsCount;

  private CompactionMetricData(List<ShowCompactResponseElement> compacts) {
    this.compacts = compacts;
  }

  static CompactionMetricData of(List<ShowCompactResponseElement> compacts) {
    CompactionMetricData data = new CompactionMetricData(Optional.ofNullable(compacts)
        .orElseGet(ImmutableList::of));
    data.init();
    return data;
  }

  private void init() {
    final Map<String, ShowCompactResponseElement> lastElements = new HashMap<>();
    poolCount = new HashMap<>();

    oldestEnqueueTime = OLDEST_TIME_NO_VALUE;
    oldestWorkingTime = OLDEST_TIME_NO_VALUE;
    oldestCleaningTime = OLDEST_TIME_NO_VALUE;
    for (ShowCompactResponseElement element : compacts) {
      final String key = element.getDbname() + "/" + element.getTablename() +
          (element.getPartitionname() != null ? "/" + element.getPartitionname() : "");

      // If new key, add the element, if there is an existing one, change to the element if the element.id is greater than old.id
      lastElements.compute(key, (k, old) -> (old == null) ? element : (element.getId() > old.getId() ? element : old));

      // find the oldest elements with initiated and working states
      String state = element.getState();
      if (TxnStore.INITIATED_RESPONSE.equals(state) && (oldestEnqueueTime > element.getEnqueueTime())) {
        oldestEnqueueTime = element.getEnqueueTime();
        poolCount.compute(element.getPoolName(), (k, old) -> (old == null) ? 1 : old + 1);
      }

      if (element.isSetStart()) {
        if (TxnStore.WORKING_RESPONSE.equals(state) && (oldestWorkingTime > element.getStart())) {
          oldestWorkingTime = element.getStart();
        }
      }

      if (element.isSetCleanerStart()) {
        if (TxnStore.CLEANING_RESPONSE.equals(state) && (oldestCleaningTime > element.getCleanerStart())) {
          oldestCleaningTime = element.getCleanerStart();
        }
      }
    }

    stateCount = lastElements
        .values()
        .stream()
        .collect(Collectors.groupingBy(ShowCompactResponseElement::getState, Collectors.counting()));

    failedCompactionPercentage = calculateFailedPercentage(stateCount);

    initiatorsCount = lastElements.values()
        .stream()
        //manually initiated compactions don't count
        .filter(e -> !MANUALLY_INITIATED_COMPACTION.equals(getThreadIdFromId(e.getInitiatorId())))
        .map(e -> getHostFromId(e.getInitiatorId()))
        .filter(e -> !NO_VAL.equals(e))
        .distinct()
        .count();
    initiatorVersionsCount = lastElements.values()
        .stream()
        .map(ShowCompactResponseElement::getInitiatorVersion)
        .filter(Objects::nonNull)
        .distinct()
        .count();

    workersCount = lastElements.values()
        .stream()
        .map(e -> getHostFromId(e.getWorkerid()))
        .filter(e -> !NO_VAL.equals(e))
        .distinct()
        .count();
    workerVersionsCount = lastElements.values()
        .stream()
        .map(ShowCompactResponseElement::getWorkerVersion)
        .filter(Objects::nonNull)
        .distinct()
        .count();
  }

  List<String> allWorkerVersionsSince(long since) {
    return compacts.stream()
        .filter(comp -> (comp.isSetEnqueueTime() && (comp.getEnqueueTime() >= since))
            || (comp.isSetStart() && (comp.getStart() >= since))
            || (comp.isSetEndTime() && (comp.getEndTime() >= since)))
        .filter(comp -> !TxnStore.DID_NOT_INITIATE_RESPONSE.equals(comp.getState()))
        .map(ShowCompactResponseElement::getWorkerVersion)
        .filter(Objects::nonNull)
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  Map<String, Long> getStateCount() {
    return new HashMap<>(stateCount);
  }

  public Map<String, Integer> getPoolCount() {
    return new HashMap<>(poolCount);
  }

  Long getOldestEnqueueTime() {
    return nullIfNotSet(oldestEnqueueTime);
  }

  Long getOldestWorkingTime() {
    return nullIfNotSet(oldestWorkingTime);
  }

  Long getOldestCleaningTime() {
    return nullIfNotSet(oldestCleaningTime);
  }

  Double getFailedCompactionPercentage() {
    return failedCompactionPercentage;
  }

  long getInitiatorsCount() {
    return initiatorsCount;
  }

  long getInitiatorVersionsCount() {
    return initiatorVersionsCount;
  }

  long getWorkersCount() {
    return workersCount;
  }

  long getWorkerVersionsCount() {
    return workerVersionsCount;
  }

  private static Long nullIfNotSet(long value) {
    if (value == OLDEST_TIME_NO_VALUE) {
      return null;
    }
    return value;
  }

  private static Double calculateFailedPercentage(Map<String, Long> stateCount) {
    long failed = unwrapToPrimitive(stateCount.get(TxnStore.FAILED_RESPONSE));
    long notInitiated = unwrapToPrimitive(stateCount.get(TxnStore.DID_NOT_INITIATE_RESPONSE));
    long succeeded = unwrapToPrimitive(stateCount.get(TxnStore.SUCCEEDED_RESPONSE));
    long refused = unwrapToPrimitive(stateCount.get(TxnStore.REFUSED_RESPONSE));

    long denominator = failed + notInitiated + refused + succeeded;
    if (denominator > 0) {
      long numerator = failed + notInitiated + refused;
      return Long.valueOf(numerator).doubleValue() / Long.valueOf(denominator).doubleValue();
    }

    return null;
  }

  private static long unwrapToPrimitive(Long value) {
    if (value == null) {
      return 0L;
    }
    return value;
  }
}
