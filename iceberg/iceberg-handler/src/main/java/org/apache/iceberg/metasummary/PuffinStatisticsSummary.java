/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.metasummary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.metasummary.SummaryMapBuilder;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;

/**
 * Collect the puffin stats summary for the table
 */
public class PuffinStatisticsSummary extends IcebergSummaryRetriever {
  private static final String STATS = "stats";
  private static final String PUFFIN_STATS_ENABLED = "puffin_enabled";
  private static final String PUFFIN_STATS_BLOB = "puffin_blobs";

  @Override
  public List<String> getFieldNames() {
    if (formatJson) {
      return Collections.singletonList(STATS);
    }
    return Arrays.asList(PUFFIN_STATS_ENABLED, PUFFIN_STATS_BLOB);
  }

  @Override
  public void getMetaSummary(Table table, MetadataTableSummary summary) {
    SummaryMapBuilder builder = new SummaryMapBuilder();
    List<StatisticsFile> statsFiles = table.statisticsFiles();
    builder.add(PUFFIN_STATS_ENABLED, statsFiles != null && !statsFiles.isEmpty());
    if (statsFiles != null && !statsFiles.isEmpty()) {
      StatisticsFile statsFile = statsFiles.get(0);
      List<BlobMetadata> blobMetadatas = statsFile.blobMetadata();
      if (blobMetadatas != null) {
        builder.add(PUFFIN_STATS_BLOB, blobMetadatas.stream().map(BlobMetadata::type)
            .collect(Collectors.joining(",")));
      }
    }
    if (builder.get(PUFFIN_STATS_BLOB, String.class) == null) {
      builder.add(PUFFIN_STATS_BLOB, null);
    }
    if (formatJson) {
      summary.addExtra(STATS, builder);
    } else {
      summary.addExtra(builder);
    }
  }
}
