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
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;

@Category(MetastoreUnitTest.class)
public class TestMultipleWorkerVersionDetection {

  private final long SINCE_EPOCH = 0L;

  @Test
  public void testCollectWorkerVersionsEmptyLists() {
    assertThat(AcidMetricService.collectWorkerVersions(null, SINCE_EPOCH), CoreMatchers.is(Collections.emptyList()));
    assertThat(AcidMetricService.collectWorkerVersions(Collections.emptyList(), SINCE_EPOCH),
        CoreMatchers.is(Collections.emptyList()));
  }

  @Test
  public void testCollectWorkerVersionsDidNotInitiateGettingFilteredOut() {
    assertThat(AcidMetricService.collectWorkerVersions(
            ImmutableList.of(
                showCompactResponse("DoNotShowUp", "did not initiate", 1L, 1L, 1L),
                showCompactResponse("1.0", "initiated", 1L, 1L, 1L)),
            SINCE_EPOCH),

        CoreMatchers.is(Collections.singletonList("1.0"))
    );
  }

  @Test
  public void testCollectWorkerVersionsNullVersionGettingFilteredOut() {
    assertThat(AcidMetricService.collectWorkerVersions(
            ImmutableList.of(
                showCompactResponse(null, "initiated", 1L, 1L, 1L)),
            SINCE_EPOCH),

        CoreMatchers.is(Collections.emptyList())
    );
  }

  @Test
  public void testCollectWorkerVersionsTimeThreshold() {
    assertThat(AcidMetricService.collectWorkerVersions(
            ImmutableList.of(
                showCompactResponse("0.0-not-shown", "initiated", 99L, null, null),
                showCompactResponse("0.1-not-shown", "initiated", 99L, 99L, null),
                showCompactResponse("0.2-not-shown", "initiated", 99L, 99L, 99L),

                showCompactResponse("1.0", "initiated", 100L, null, null),
                showCompactResponse("1.1", "working", 99L, 100L, null),
                showCompactResponse("1.2", "succeeded", 99L, 99L, 100L)
            ),
            100),

        CoreMatchers.is(ImmutableList.of("1.0", "1.1", "1.2"))
    );
  }

  @Test
  public void testCollectWorkerVersionsSortedAndAvoidDuplicates() {
    assertThat(AcidMetricService.collectWorkerVersions(
            ImmutableList.of(
                showCompactResponse("2.0", "initiated", 1L, null, null),
                showCompactResponse("2.1", "initiated", 1L, null, null),
                showCompactResponse("2.10", "initiated", 1L, null, null),
                showCompactResponse("2.2", "initiated", 1L, null, null),
                showCompactResponse("3.0", "working", 1L, null, null),
                showCompactResponse("1.0", "initiated", 1L, null, null),
                showCompactResponse("1.0", "working", 1L, null, null)
            ),
            SINCE_EPOCH),

        CoreMatchers.is(ImmutableList.of("1.0", "2.0", "2.1", "2.10", "2.2", "3.0"))
    );
  }

  private static ShowCompactResponseElement showCompactResponse(String workerVersion, String state,
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
