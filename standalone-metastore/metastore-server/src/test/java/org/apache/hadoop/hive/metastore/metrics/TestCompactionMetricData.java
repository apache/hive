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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Category(MetastoreUnitTest.class)
public class TestCompactionMetricData {

  private static final long SINCE_EPOCH = 0L;

  private static final String INITIATED = "initiated";
  private static final String WORKING = "working";
  private static final String READY_FOR_CLEANING = "ready for cleaning";
  private static final String DID_NOT_INITIATE = "did not initiate";
  private static final String SUCCEEDED = "succeeded";
  private static final String FAILED = "failed";

  @Test
  public void testStateCountsCountedCorrectly() {
    assertThat(
        CompactionMetricData.of(null)
            .getStateCount(),
        is(emptyMap()));

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", null, INITIATED, CompactionType.MINOR),
                    aCompaction(2, "t1", null, INITIATED, CompactionType.MAJOR),
                    aCompaction(3, "t2", "part1", WORKING, CompactionType.MINOR),
                    aCompaction(5, "t2", "part1", WORKING, CompactionType.MAJOR),
                    aCompaction(6, "t2", "part2", WORKING, CompactionType.MAJOR),
                    aCompaction(7, "t3", null, READY_FOR_CLEANING, CompactionType.MAJOR)
                ))
            .getStateCount(),
        is(ImmutableMap.of(
            INITIATED, 1L,
            WORKING, 2L,
            READY_FOR_CLEANING, 1L
        )));
  }

  @Test
  public void testOldestEnqueuedValueCalculatedCorrectly() {
    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(3, "t2", "part1", WORKING, CompactionType.MINOR, 90L, null, null)
                ))
            .getOldestEnqueueTime(),
        nullValue());

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", null, INITIATED, CompactionType.MINOR, 150L, null, null),
                    aCompaction(2, "t1", null, INITIATED, CompactionType.MAJOR, 100L, null, null),
                    aCompaction(3, "t2", "part1", WORKING, CompactionType.MINOR, 90L, null, null),
                    aCompaction(6, "t2", "part2", DID_NOT_INITIATE, CompactionType.MAJOR, 50L, null, null),
                    aCompaction(7, "t3", null, READY_FOR_CLEANING, CompactionType.MAJOR, 300L, null, null)
                ))
            .getOldestEnqueueTime(),
        is(100L));
  }

  @Test
  public void testOldestWorkingValueCalculatedCorrectly() {
    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(3, "t2", "part1", INITIATED, CompactionType.MINOR, null, 90L, null)
                ))
            .getOldestWorkingTime(),
        nullValue());

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", null, INITIATED, CompactionType.MINOR, null, 150L, null),
                    aCompaction(2, "t1", null, INITIATED, CompactionType.MAJOR, null, 100L, null),
                    aCompaction(4, "t2", "part1", WORKING, CompactionType.MINOR, null, 90L, null),
                    aCompaction(3, "t2", "part1", WORKING, CompactionType.MINOR, null, 70L, null),
                    aCompaction(6, "t2", "part2", DID_NOT_INITIATE, CompactionType.MAJOR, null, 50L, null),
                    aCompaction(7, "t3", null, READY_FOR_CLEANING, CompactionType.MAJOR, null, 300L, null)
                ))
            .getOldestWorkingTime(),
        is(70L));
  }

  @Test
  public void testOldestCleaningValueCalculatedCorrectly() {
    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(3, "t2", "part1", INITIATED, CompactionType.MINOR, null, null, 90L)
                ))
            .getOldestCleaningTime(),
        nullValue());

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", null, INITIATED, CompactionType.MINOR, null, null, 150L),
                    aCompaction(2, "t1", null, READY_FOR_CLEANING, CompactionType.MAJOR, null, null, 100L),
                    aCompaction(4, "t2", "part1", WORKING, CompactionType.MINOR, null, null, 90L),
                    aCompaction(3, "t2", "part1", WORKING, CompactionType.MINOR, null, null, 70L),
                    aCompaction(7, "t3", null, READY_FOR_CLEANING, CompactionType.MAJOR, null, null, 300L)
                ))
            .getOldestCleaningTime(),
        is(100L));
  }

  @Test
  public void testFailedPercentageCalculatedCorrectly() {
    assertThat(
        CompactionMetricData.of(
                ImmutableList.of())
            .getFailedCompactionPercentage(),
        nullValue());

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(aCompaction(1, "t1", "p1", SUCCEEDED, CompactionType.MINOR)))
            .getFailedCompactionPercentage(),
        is(0.0D));

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", "p1", FAILED, CompactionType.MINOR),
                    aCompaction(2, "t2", "p1", SUCCEEDED, CompactionType.MINOR)))
            .getFailedCompactionPercentage(),
        is(0.5D));

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", "p1", FAILED, CompactionType.MINOR),
                    aCompaction(2, "t2", "p1", DID_NOT_INITIATE, CompactionType.MINOR),
                    aCompaction(3, "t3", "p1", SUCCEEDED, CompactionType.MINOR),
                    aCompaction(4, "t4", "p1", SUCCEEDED, CompactionType.MINOR)))
            .getFailedCompactionPercentage(),
        is(0.5D));

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", "p1", FAILED, CompactionType.MINOR),
                    aCompaction(2, "t2", "p1", DID_NOT_INITIATE, CompactionType.MINOR)))
            .getFailedCompactionPercentage(),
        is(1.0D));

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", "p1", FAILED, CompactionType.MINOR)))
            .getFailedCompactionPercentage(),
        is(1.0D));

    assertThat(
        CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, "t1", "p1", DID_NOT_INITIATE, CompactionType.MINOR)))
            .getFailedCompactionPercentage(),
        is(1.0D));
  }

  @Test
  public void testInitiatorCountCalculatedCorrectly() {
    assertThat(CompactionMetricData.of(Collections.emptyList())
            .getInitiatorsCount(),
        is(0L));
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, null, null, null, (String) null),
                    aCompaction(2, "host1-initiator", null, null, (String) null),
                    aCompaction(3, "host2-initiator-manual", null, null, (String) null),
                    aCompaction(4, "host3-initiator", null, null, (String) null)))
            .getInitiatorsCount(),
        is(2L));
  }

  @Test
  public void testInitiatorVersionsCalculatedCorrectly() {
    assertThat(CompactionMetricData.of(Collections.emptyList())
            .getInitiatorVersionsCount(),
        is(0L));
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, null, "1.0", null, (String) null),
                    aCompaction(2, null, "3.0", null, (String) null),
                    aCompaction(3, null, "4.0", null, (String) null),
                    aCompaction(4, null, null, null, (String) null),
                    aCompaction(5, null, "4.0", null, (String) null)))
            .getInitiatorVersionsCount(),
        is(3L));
  }

  @Test
  public void testWorkerCountCalculatedCorrectly() {
    assertThat(CompactionMetricData.of(Collections.emptyList())
            .getWorkersCount(),
        is(0L));
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, null, null, null, "4.0"),
                    aCompaction(2, null, null, "host1-worker", "4.0"),
                    aCompaction(3, null, null, "host2-worker", "4.0")))
            .getWorkersCount(),
        is(2L));
  }

  @Test
  public void testWorkerVersionsCalculatedCorrectly() {
    assertThat(CompactionMetricData.of(Collections.emptyList())
            .getWorkerVersionsCount(),
        is(0L));
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(1, null, null, null, "1.0"),
                    aCompaction(2, null, null, null, "3.0"),
                    aCompaction(3, null, null, null, "4.0"),
                    aCompaction(4, null, null, null, (String) null),
                    aCompaction(5, null, null, null, "4.0")))
            .getWorkerVersionsCount(),
        is(3L));
  }

  @Test
  public void testCollectWorkerVersionsEmptyLists() {
    assertThat(CompactionMetricData.of(Collections.emptyList()).allWorkerVersionsSince(SINCE_EPOCH),

        is(Collections.emptyList()));
  }

  @Test
  public void testCollectWorkerVersionsDidNotInitiateGettingFilteredOut() {
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction("DoNotShowUp", DID_NOT_INITIATE, 1L, 1L, 1L),
                    aCompaction("1.0", INITIATED, 1L, 1L, 1L)))
            .allWorkerVersionsSince(SINCE_EPOCH),

        is(Collections.singletonList("1.0"))
    );
  }

  @Test
  public void testCollectWorkerVersionsNullVersionGettingFilteredOut() {
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction(null, INITIATED, 1L, 1L, 1L)))
            .allWorkerVersionsSince(SINCE_EPOCH),

        is(Collections.emptyList())
    );
  }

  @Test
  public void testCollectWorkerVersionsTimeThreshold() {
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction("0.0-not-shown", INITIATED, 99L, null, null),
                    aCompaction("0.1-not-shown", INITIATED, 99L, 99L, null),
                    aCompaction("0.2-not-shown", INITIATED, 99L, 99L, 99L),

                    aCompaction("1.0", INITIATED, 100L, null, null),
                    aCompaction("1.1", WORKING, 99L, 100L, null),
                    aCompaction("1.2", SUCCEEDED, 99L, 99L, 100L)
                ))
            .allWorkerVersionsSince(100),

        is(ImmutableList.of("1.0", "1.1", "1.2"))
    );
  }

  @Test
  public void testCollectWorkerVersionsSortedAndAvoidDuplicates() {
    assertThat(CompactionMetricData.of(
                ImmutableList.of(
                    aCompaction("2.0", INITIATED, 1L, null, null),
                    aCompaction("2.1", INITIATED, 1L, null, null),
                    aCompaction("2.10", INITIATED, 1L, null, null),
                    aCompaction("2.2", INITIATED, 1L, null, null),
                    aCompaction("3.0", WORKING, 1L, null, null),
                    aCompaction("1.0", INITIATED, 1L, null, null),
                    aCompaction("1.0", WORKING, 1L, null, null)
                ))
            .allWorkerVersionsSince(SINCE_EPOCH),

        is(ImmutableList.of("1.0", "2.0", "2.1", "2.10", "2.2", "3.0"))
    );
  }

  private static ShowCompactResponseElement aCompaction(long id, String table, String partition, String state,
      CompactionType type) {
    ShowCompactResponseElement e = new ShowCompactResponseElement("db_name", table, type, state);
    e.setId(id);
    e.setPartitionname(partition);
    return e;
  }

  private static ShowCompactResponseElement aCompaction(long id,
      String initiatorId, String initiatorVersion,
      String workerId, String workerVersion) {
    ShowCompactResponseElement e = new ShowCompactResponseElement("db_name", UUID.randomUUID().toString(),
        CompactionType.MAJOR, INITIATED);
    e.setId(id);

    e.setInitiatorId(initiatorId);
    e.setInitiatorVersion(initiatorVersion);

    e.setWorkerid(workerId);
    e.setWorkerVersion(workerVersion);

    return e;
  }

  private static ShowCompactResponseElement aCompaction(long id, String table, String partition, String state,
      CompactionType type, Long enqueuedTime, Long startTime, Long cleanerStart) {
    ShowCompactResponseElement e = new ShowCompactResponseElement("db_name", table, type, state);
    e.setId(id);
    e.setPartitionname(partition);

    if (enqueuedTime != null) {
      e.setEnqueueTime(enqueuedTime);
    }

    if (startTime != null) {
      e.setStart(startTime);
    }

    if (cleanerStart != null) {
      e.setCleanerStart(cleanerStart);
    }

    return e;
  }

  private static ShowCompactResponseElement aCompaction(String workerVersion, String state,
      Long enqueuedTime, Long startTime, Long endTime) {

    ShowCompactResponseElement e = new ShowCompactResponseElement("db_name", "table_name", CompactionType.MINOR, state);
    e.setWorkerVersion(workerVersion);

    if (enqueuedTime != null) {
      e.setEnqueueTime(enqueuedTime);
    }

    if (startTime != null) {
      e.setStart(startTime);
    }

    if (endTime != null) {
      e.setEndTime(endTime);
    }

    return e;
  }
}
