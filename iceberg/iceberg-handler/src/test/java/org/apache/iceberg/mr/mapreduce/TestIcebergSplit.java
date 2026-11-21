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

package org.apache.iceberg.mr.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergSplit {
  private static final Schema SCHEMA = new Schema(
      optional(1, "station", Types.StringType.get()),
      optional(2, "measure", Types.DoubleType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).bucket("station", 4).build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private CombinedScanTask taskGroup;

  // a task group of several data files, each with a position delete file
  @Before
  public void planTaskGroup() throws IOException {
    File location = temp.newFolder("table");
    Assert.assertTrue(location.delete());
    Table table = new HadoopTables(new Configuration()).create(SCHEMA, SPEC,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"), location.toString());
    List<DataFile> dataFiles = Lists.newArrayList();
    AppendFiles append = table.newFastAppend();
    for (int bucket = 0; bucket < 4; bucket++) {
      DataFile dataFile = DataFiles.builder(SPEC)
          .withPath(location + "/data/station_bucket=" + bucket + "/data-" + bucket + ".parquet")
          .withPartitionPath("station_bucket=" + bucket)
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(1024)
          .withRecordCount(100)
          .build();
      dataFiles.add(dataFile);
      append.appendFile(dataFile);
    }
    append.commit();
    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      rowDelta.addDeletes(FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath(dataFile.location() + "-deletes.parquet")
          .withFormat(FileFormat.PARQUET)
          .withPartition(dataFile.partition())
          .withFileSizeInBytes(128)
          .withRecordCount(1)
          .build());
    }
    rowDelta.commit();
    // the default split size combines the small files into one task group
    try (CloseableIterable<CombinedScanTask> tasks = table.newScan().planTasks()) {
      taskGroup = tasks.iterator().next();
    }
    Assert.assertEquals(4, taskGroup.tasks().size());
  }

  @Test
  public void testRoundTripWithEveryCodec() throws IOException {
    int uncompressed = write(split("none")).length;
    for (String codec : new String[] {"none", "snappy", "zstd"}) {
      byte[] payload = write(split(codec));
      if (!"none".equals(codec)) {
        Assert.assertTrue(codec + " compresses the payload", payload.length < uncompressed);
      }
      assertSameTasks(read(payload).taskGroup());
    }
  }

  @Test
  public void testSplitWithoutConfUsesTheDefaultCodec() throws IOException {
    String defaultCodec = HiveConf.ConfVars.HIVE_ICEBERG_SPLIT_COMPRESSION_CODEC.getDefaultValue();
    byte[] payload = write(new IcebergSplit(null, taskGroup));
    Assert.assertArrayEquals(write(split(defaultCodec)), payload);
    assertSameTasks(read(payload).taskGroup());
  }

  @Test
  public void testUnknownCodecIdFails() throws IOException {
    byte[] payload = write(split("none"));
    payload[0] = 7;
    IOException failure = Assert.assertThrows(IOException.class, () -> read(payload));
    Assert.assertEquals("Unknown Iceberg split codec id 7", failure.getMessage());
  }

  private IcebergSplit split(String codec) {
    Configuration conf = new Configuration(false);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_SPLIT_COMPRESSION_CODEC, codec);
    return new IcebergSplit(conf, taskGroup);
  }

  private static byte[] write(IcebergSplit split) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    split.write(out);
    return Arrays.copyOf(out.getData(), out.getLength());
  }

  private static IcebergSplit read(byte[] payload) throws IOException {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(payload, payload.length);
    IcebergSplit split = new IcebergSplit();
    split.readFields(in);
    return split;
  }

  private void assertSameTasks(ScanTaskGroup<FileScanTask> actual) {
    List<FileScanTask> expectedTasks = Lists.newArrayList(taskGroup.tasks());
    List<FileScanTask> actualTasks = Lists.newArrayList(actual.tasks());
    Assert.assertEquals(expectedTasks.size(), actualTasks.size());
    for (int task = 0; task < expectedTasks.size(); task++) {
      FileScanTask expected = expectedTasks.get(task);
      FileScanTask read = actualTasks.get(task);
      Assert.assertEquals(expected.file().location(), read.file().location());
      Assert.assertEquals(expected.file().dataSequenceNumber(), read.file().dataSequenceNumber());
      Assert.assertEquals(expected.start(), read.start());
      Assert.assertEquals(expected.length(), read.length());
      Assert.assertEquals(expected.deletes().size(), read.deletes().size());
      for (int delete = 0; delete < expected.deletes().size(); delete++) {
        DeleteFile expectedDelete = expected.deletes().get(delete);
        Assert.assertEquals(expectedDelete.location(), read.deletes().get(delete).location());
        Assert.assertEquals(expectedDelete.dataSequenceNumber(), read.deletes().get(delete).dataSequenceNumber());
      }
    }
  }
}
