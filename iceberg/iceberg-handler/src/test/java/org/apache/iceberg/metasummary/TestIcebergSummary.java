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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestIcebergSummary {
  private static final HadoopTables TABLES = new HadoopTables();
  private static final Schema SCHEMA =
      new Schema(
          required(1, "foo", Types.IntegerType.get()),
          required(2, "bar", Types.StringType.get()),
          optional(3, "alist", Types.ListType.ofOptional(5, Types.StringType.get())),
          optional(4, "amap", Types.MapType.ofOptional(6, 7, Types.IntegerType.get(), Types.StringType.get())));

  @TempDir
  public Path tableDir;
  @TempDir
  public Path dataDir;

  private final Configuration conf = MetastoreConf.newMetastoreConf();

  @Test
  public void testGetMetadataSummary() throws Exception {
    MetadataSummary summary = new MetadataSummary();
    summary.initialize(conf, false);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("foo", 16).build();
    Map<String, String> props = Maps.newHashMap();
    props.put("history.expire.min-snapshots-to-keep", "7");
    String location = tableDir.toAbsolutePath() + "/test_metadata_summary";
    TABLES.create(SCHEMA, spec, props, location);
    Table table = TABLES.load(location);
    AppendFiles append = table.newAppend();
    String data1 = dataDir.toAbsolutePath() + "/data1.parquet";
    String data2 = dataDir.toAbsolutePath() + "/data2.parquet";
    Files.write(Paths.get(data1), Lists.newArrayList(), StandardCharsets.UTF_8);
    Files.write(Paths.get(data2), Lists.newArrayList(), StandardCharsets.UTF_8);
    PartitionData data = new PartitionData(spec.partitionType());
    data.set(0, 1);

    append.appendFile(DataFiles.builder(spec)
        .withPath(data1)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withPartition(data)
        .build());
    append.commit();
    data = new PartitionData(spec.partitionType());
    data.set(0, 2);
    table.newAppend()
        .appendFile(DataFiles.builder(spec)
            .withPath(data2)
            .withFileSizeInBytes(20)
            .withRecordCount(2)
            .withPartition(data)
            .build())
        .commit();

    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("b1", first).commit();
    table.manageSnapshots().createTag("t1", first).commit();
    table.manageSnapshots().createTag("t2", first).commit();
    MetadataTableSummary tableSummary = new MetadataTableSummary();
    summary.getMetaSummary(table, tableSummary);

    Assertions.assertEquals(1, tableSummary.getPartitionColumnCount());
    Assertions.assertEquals(2, tableSummary.getNumFiles());
    Assertions.assertEquals(2, tableSummary.getPartitionCount());
    Assertions.assertEquals(3, tableSummary.getNumRows());
    Assertions.assertEquals(4, tableSummary.getColCount());
    Assertions.assertEquals(1, tableSummary.getArrayColumnCount());
    Assertions.assertEquals(1, tableSummary.getMapColumnCount());
    Assertions.assertEquals(0, tableSummary.getStructColumnCount());
    Assertions.assertEquals(30, tableSummary.getTotalSize());

    Map<String, Object> extraSummary = tableSummary.getExtraSummary();
    Assertions.assertEquals(2, extraSummary.get(MetadataSummary.NUM_SNAPSHOTS));
    Assertions.assertEquals(2, extraSummary.get(MetadataSummary.NUM_TAGS));
    Assertions.assertEquals(2, extraSummary.get(MetadataSummary.NUM_BRANCHES));
    Assertions.assertEquals(-1L, extraSummary.get(MetadataSummary.SNAPSHOT_MAX_AGE));
    Assertions.assertEquals(7L, extraSummary.get(MetadataSummary.SNAPSHOT_MIN_KEEP));

    File directory = new File(table.location());
    List<File> manifestFiles = listManifestFiles(directory);
    Assertions.assertEquals(manifestFiles.size(), extraSummary.get(MetadataSummary.NUM_MANIFESTS));
    Assertions.assertEquals(manifestFiles.stream().mapToLong(File::length).sum(),
        extraSummary.get(MetadataSummary.MANIFESTS_SIZE));
  }

  @Test
  public void testTablePropsSummary() {
    TablePropertySummary summary = new TablePropertySummary();
    summary.initialize(conf, false);
    Map<String, String> props = Maps.newHashMap();
    props.put("write.format.default", "orc");
    props.put("write.delete.format.default", "parquet");
    props.put("write.distribution-mode", "hash");
    props.put("write.wap.enabled", "true");
    props.put("format-version", "2");
    props.put("write.delete.mode", "merge-on-read");
    props.put("write.update.mode", "copy-on-write");
    String location = tableDir.toAbsolutePath() + "/test_tabprops_summary";
    TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), props, location);
    Table table = TABLES.load(location);
    MetadataTableSummary tableSummary = new MetadataTableSummary();
    summary.getMetaSummary(table, tableSummary);

    Map<String, Object> extraSummary = tableSummary.getExtraSummary();
    Assertions.assertEquals("orc", extraSummary.get("write.format.default"));
    Assertions.assertEquals("parquet", extraSummary.get("write.delete.format.default"));
    Assertions.assertEquals("hash", extraSummary.get("write.distribution-mode"));
    Assertions.assertEquals("true", extraSummary.get("write.wap.enabled"));
    Assertions.assertEquals("merge-on-read", extraSummary.get("write.delete.mode"));
    Assertions.assertEquals("copy-on-write", extraSummary.get("write.update.mode"));
    Assertions.assertEquals(2, extraSummary.get("version"));
  }

  List<File> listManifestFiles(File tableDirToList) {
    return Lists.newArrayList(
        new File(tableDirToList, "metadata")
            .listFiles((dir, name) ->
                !name.startsWith("snap") && name.endsWith(".avro")));
  }
}
