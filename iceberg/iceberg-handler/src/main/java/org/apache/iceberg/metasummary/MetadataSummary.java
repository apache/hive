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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.metasummary;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.metasummary.SummaryMapBuilder;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.SNAPSHOT_COUNT;

/**
 * Collect the metadata summary for the table, like the number of snapshots, data files
 * or the partition count.
 */
public class MetadataSummary extends IcebergSummaryRetriever {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataSummary.class);
  public static final String SNAPSHOT_MAX_AGE = "snapshotMaxAge";
  public static final String SNAPSHOT_MIN_KEEP = "snapshotMinKeep";
  public static final String NUM_SNAPSHOTS = "numSnapshots";
  public static final String NUM_MANIFESTS = "numManifests";
  public static final String NUM_METADATA_FILES = "numMetadataFiles";
  public static final String MANIFESTS_SIZE = "manifestsSize";
  public static final String NUM_BRANCHES = "numBranches";
  public static final String NUM_TAGS = "numTags";

  @Override
  public List<String> getFieldNames() {
    if (formatJson) {
      return Collections.singletonList("metadata");
    }
    return Arrays.asList(NUM_SNAPSHOTS, NUM_METADATA_FILES, NUM_MANIFESTS,
        MANIFESTS_SIZE, SNAPSHOT_MAX_AGE, SNAPSHOT_MIN_KEEP, NUM_BRANCHES, NUM_TAGS);
  }

  @Override
  public void getMetaSummary(Table table, MetadataTableSummary metaSummary) {
    basicMetadataSummary(table, metaSummary);
    SummaryMapBuilder builder = new SummaryMapBuilder();
    Table metadataEntries = MetadataTableUtils
        .createMetadataTableInstance(table, MetadataTableType.METADATA_LOG_ENTRIES);
    try (CloseableIterable<ScanTask> iterable = metadataEntries.newBatchScan().planFiles()) {
      List<?> files = Lists.newArrayList(CloseableIterable.transform(iterable,
          t -> ((FileScanTask) t).file()));
      builder.add(NUM_METADATA_FILES, files.size());
    } catch (IOException e) {
      LOG.warn("Error while listing the metadata files, table: " + table.name(), e);
      builder.add(NUM_METADATA_FILES, 1);
    }
    int numManifests = 0;
    long manifestsSize = 0;
    if (table.currentSnapshot() != null) {
      List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
      numManifests = manifests.size();
      manifestsSize = manifests.stream().map(ManifestFile::length).mapToLong(Long::longValue).sum();
    }

    branchOrTagSummary(table, builder);
    Map<String, String> properties = table.properties();
    builder
        .add(NUM_SNAPSHOTS, Integer.parseInt(properties.getOrDefault(SNAPSHOT_COUNT,
            "" + IteratorUtils.size(table.snapshots().iterator()))))
        .add(NUM_MANIFESTS, numManifests)
        .add(MANIFESTS_SIZE, manifestsSize)
        .add(SNAPSHOT_MAX_AGE,
            Long.parseLong(properties.getOrDefault("history.expire.max-snapshot-age-ms", "-1")))
        .add(SNAPSHOT_MIN_KEEP,
            Long.parseLong(properties.getOrDefault("history.expire.min-snapshots-to-keep", "-1")));
    if (formatJson) {
      metaSummary.addExtra("metadata", builder);
    } else {
      metaSummary.addExtra(builder);
    }
  }

  private void basicMetadataSummary(Table table, MetadataTableSummary metaSummary) {
    Map<String, String> summary = table.currentSnapshot().summary();
    metaSummary.setNumRows(Long.parseLong(summary.getOrDefault(SnapshotSummary.TOTAL_RECORDS_PROP, "0")));
    metaSummary.setTotalSize(Long.parseLong(summary.getOrDefault(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0")));
    metaSummary.setPartitionColumnCount(table.spec().fields().size());
    metaSummary.setNumFiles(Long.parseLong(summary.getOrDefault(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")));
    List<Types.NestedField> columns = table.schema().columns();
    metaSummary.columnSummary(columns.size(),
        (int) columns.stream().filter(col -> col.type().isListType()).count(),
        (int) columns.stream().filter(col -> col.type().isStructType()).count(),
        (int) columns.stream().filter(col -> col.type().isMapType()).count());

    if (table.spec().isPartitioned()) {
      PartitionsTable partitionsTable =
          (PartitionsTable) MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
      try (CloseableIterable<FileScanTask> iterable = partitionsTable.newScan().planFiles()) {
        long partitionCount = 0;
        for (FileScanTask fileScanTask : iterable) {
          partitionCount += fileScanTask.file().recordCount();
        }
        metaSummary.setPartitionCount((int) partitionCount);
      } catch (Exception e) {
        LOG.warn("Error listing the partitions in table: " + table.name(), e);
        metaSummary.setPartitionCount(0);
      }
    }
  }

  private void branchOrTagSummary(Table table, SummaryMapBuilder builder) {
    Map<String, SnapshotRef> refs = table.refs();
    AtomicInteger numBranches = new AtomicInteger(0);
    AtomicInteger numTags = new AtomicInteger(0);
    if (refs != null) {
      refs.forEach((key, value) -> {
        if (value.isBranch()) {
          numBranches.getAndIncrement();
        } else {
          numTags.getAndIncrement();
        }
      });
    }
    builder.add(NUM_BRANCHES, numBranches.get()).add(NUM_TAGS, numTags.get());
  }
}
