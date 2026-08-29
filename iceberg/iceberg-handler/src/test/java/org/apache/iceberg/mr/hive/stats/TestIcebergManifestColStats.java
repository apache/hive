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

package org.apache.iceberg.mr.hive.stats;

import java.util.OptionalLong;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.iceberg.mr.hive.stats.IcebergManifestColStats.ColumnBounds;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

/** What the two sides of an entry state together, and what neither of them may invent. */
public class TestIcebergManifestColStats {

  private static final Types.NestedField ID = Types.NestedField.optional(1, "id", Types.LongType.get());
  private static final Types.NestedField NAME = Types.NestedField.optional(2, "name", Types.StringType.get());
  private static final Types.NestedField AT =
      Types.NestedField.optional(3, "at", Types.TimestampType.withoutZone());

  @Test
  public void aTimestampBoundStaysABoundThroughTheSecondItIsTruncatedTo() {
    // Iceberg counts microseconds, the entry counts seconds. Rounding both ends the same way would
    // put a value of the truncated second outside what the entry states, which a planner reads as
    // a value that cannot exist
    long micros = 10_500_000L;
    ColumnStatisticsObj statsObj = IcebergManifestColStats.toColumnStats(
        AT, new ColumnBounds(3, micros, micros, 0L, true), OptionalLong.of(1));

    TimestampColumnStatsData stats = statsObj.getStatsData().getTimestampStats();
    Assert.assertEquals("the low rounds down to stay at or below every value",
        10L, stats.getLowValue().getSecondsSinceEpoch());
    Assert.assertEquals("and the high rounds up to stay at or above every value",
        11L, stats.getHighValue().getSecondsSinceEpoch());
  }

  @Test
  public void aTimestampBoundBeforeTheEpochStillRoundsOutward() {
    long micros = -10_500_000L;
    ColumnStatisticsObj statsObj = IcebergManifestColStats.toColumnStats(
        AT, new ColumnBounds(3, micros, micros, 0L, true), OptionalLong.of(1));

    TimestampColumnStatsData stats = statsObj.getStatsData().getTimestampStats();
    Assert.assertEquals(-11L, stats.getLowValue().getSecondsSinceEpoch());
    Assert.assertEquals(-10L, stats.getHighValue().getSecondsSinceEpoch());
  }

  @Test
  public void theBoundsAndTheDistinctCountBecomeOneEntry() {
    ColumnStatisticsObj statsObj = IcebergManifestColStats.toColumnStats(
        ID, new ColumnBounds(1, 1L, 100L, 3L, true), OptionalLong.of(7));

    Assert.assertEquals("id", statsObj.getColName());
    Assert.assertEquals("bigint", statsObj.getColType());
    Assert.assertEquals(1L, statsObj.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(100L, statsObj.getStatsData().getLongStats().getHighValue());
    Assert.assertEquals("the manifests state how many rows held no value",
        3L, statsObj.getStatsData().getLongStats().getNumNulls());
    Assert.assertEquals("the blob states how many were distinct",
        7L, statsObj.getStatsData().getLongStats().getNumDVs());
  }

  @Test
  public void withoutADistinctCountThereIsNoEntry() {
    // no manifest has ever held one, and an entry cannot state a number nothing measured
    Assert.assertNull(IcebergManifestColStats.toColumnStats(
        ID, new ColumnBounds(1, 1L, 100L, 3L, true), OptionalLong.empty()));
  }

  @Test
  public void withoutANullCountThereIsNoEntry() {
    // a file stated none of its own, so the sum describes fewer rows than the scan reads
    Assert.assertNull(IcebergManifestColStats.toColumnStats(
        ID, new ColumnBounds(1, 1L, 100L, 0L, false), OptionalLong.of(7)));
  }

  @Test
  public void anEntryStandsWithoutBoundsTheFilesWithheld() {
    ColumnStatisticsObj statsObj = IcebergManifestColStats.toColumnStats(
        ID, new ColumnBounds(1, null, null, 3L, true), OptionalLong.of(7));

    Assert.assertFalse("a bound nothing stated is left unset", statsObj.getStatsData().getLongStats().isSetLowValue());
    Assert.assertEquals(3L, statsObj.getStatsData().getLongStats().getNumNulls());
  }

  @Test
  public void aTypeWhoseBoundsAreStoredTruncatedStatesNothingHere() {
    Assert.assertFalse("a string bound is a prefix of a value, not a value",
        IcebergManifestColStats.hasFullBounds(NAME.type()));
    Assert.assertNull(IcebergManifestColStats.toColumnStats(
        NAME, new ColumnBounds(2, null, null, 1L, true), OptionalLong.of(7)));
  }

  @Test
  public void theTypesIcebergStoresWholeAreTheOnesRead() {
    Assert.assertTrue(IcebergManifestColStats.hasFullBounds(Types.LongType.get()));
    Assert.assertTrue(IcebergManifestColStats.hasFullBounds(Types.DoubleType.get()));
    Assert.assertTrue(IcebergManifestColStats.hasFullBounds(Types.DateType.get()));
    Assert.assertFalse(IcebergManifestColStats.hasFullBounds(Types.BinaryType.get()));
  }
}
