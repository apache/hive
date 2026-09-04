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

import java.util.Map;
import java.util.OptionalLong;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * What a blob states about itself in the table metadata, where a read finds it without opening
 * the file. Only the distinct count travels this way: it is a number of a fixed size that no other
 * source holds, while the manifests already state every other scalar a scan needs, and what stays
 * in the blob - the sketch the count is estimated from, and a histogram - is not fixed in size,
 * and metadata every engine parses is no place for it.
 *
 * <p>It is stated under the name Iceberg gives it on its own sketch blobs, so that a table this
 * writes is read by another engine as its own.
 */
public final class IcebergColStatsProperties {

  /** The property Iceberg's own statistics blobs state a distinct count under. */
  private static final String NDV = "ndv";

  private IcebergColStatsProperties() {
  }

  /** What the entry states of itself, for the blob that carries it. */
  public static Map<String, String> of(ColumnStatisticsObj statsObj) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    numDVs(statsObj.getStatsData()).ifPresent(ndv -> properties.put(NDV, String.valueOf(ndv)));
    return properties.build();
  }

  /** The distinct count the blob states, absent when it states none. */
  public static OptionalLong ndv(BlobMetadata blob) {
    return asLong(blob.properties().get(NDV));
  }

  /** A boolean holds its own counts and a binary has no distinct count to state. */
  private static OptionalLong numDVs(ColumnStatisticsData data) {
    return switch (data.getSetField()) {
      case LONG_STATS -> OptionalLong.of(data.getLongStats().getNumDVs());
      case DOUBLE_STATS -> OptionalLong.of(data.getDoubleStats().getNumDVs());
      case STRING_STATS -> OptionalLong.of(data.getStringStats().getNumDVs());
      case DECIMAL_STATS -> OptionalLong.of(data.getDecimalStats().getNumDVs());
      case DATE_STATS -> OptionalLong.of(data.getDateStats().getNumDVs());
      case TIMESTAMP_STATS -> OptionalLong.of(data.getTimestampStats().getNumDVs());
      case null, default -> OptionalLong.empty();
    };
  }

  private static OptionalLong asLong(String stated) {
    if (stated == null) {
      return OptionalLong.empty();
    }
    try {
      return OptionalLong.of(Long.parseLong(stated));
    } catch (NumberFormatException e) {
      // another writer's property, or a damaged one: the blob still holds the entry itself
      return OptionalLong.empty();
    }
  }
}
