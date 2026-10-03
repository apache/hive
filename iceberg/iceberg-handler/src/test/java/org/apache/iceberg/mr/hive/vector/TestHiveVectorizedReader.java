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

package org.apache.iceberg.mr.hive.vector;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapBufferOrBuffers;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TestCachingDeleteLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat.CompatibilityTaskAttemptContextImpl;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.apache.iceberg.mr.hive.vector.TestHiveIcebergVectorization.prepareMockJob;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

public class TestHiveVectorizedReader {

  private static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(2, "id", Types.LongType.get()),
      required(3, "date", Types.StringType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private TestHelper helper;
  private InputFormatConfig.ConfigBuilder builder;

  private final FileFormat fileFormat = FileFormat.PARQUET;

  @Before
  public void before() throws IOException, HiveException {
    File location = temp.newFolder(fileFormat.name());
    Assert.assertTrue(location.delete());

    Configuration conf = prepareMockJob(SCHEMA, new Path(location.toString()));
    conf.set(CatalogUtil.ICEBERG_CATALOG_TYPE, Catalogs.LOCATION);
    HadoopTables tables = new HadoopTables(conf);

    helper = new TestHelper(conf, tables, location.toString(), SCHEMA, null, fileFormat, temp);
    builder = new InputFormatConfig.ConfigBuilder(conf).readFrom(location.toString())
      .useHiveRows();
  }

  @Test
  public void testRecordReaderShouldReuseFooter() throws IOException, InterruptedException {
    helper.createUnpartitionedTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);

    TaskAttemptContext context = new CompatibilityTaskAttemptContextImpl(builder.conf(), new TaskAttemptID(), null);
    IcebergInputFormat<?> inputFormat = new IcebergInputFormat<>();
    List<InputSplit> splits = inputFormat.getSplits(context);

    try (MockedStatic<ParquetFileReader> mockedParquetFileReader = Mockito.mockStatic(ParquetFileReader.class,
        Mockito.CALLS_REAL_METHODS)) {
      for (InputSplit split : splits) {
        try (RecordReader<Void, ?> r = inputFormat.createRecordReader(split, context)) {
          r.initialize(split, context);
          r.nextKeyValue();
        }
      }
      mockedParquetFileReader.verify(() -> ParquetFileReader.open(any(InputFile.class),
              any(ParquetReadOptions.class)), times(1)
      );
    }
  }

  @Test
  public void testDeletionVectorIsReadThroughInjectedMetadataCache() throws Exception {
    File location = temp.newFolder("v3");
    Assert.assertTrue(location.delete());
    JobConf job = prepareMockJob(SCHEMA, new Path(location.toString()));
    job.set(CatalogUtil.ICEBERG_CATALOG_TYPE, Catalogs.LOCATION);
    TestHelper v3Helper = new TestHelper(job, new HadoopTables(job), location.toString(), SCHEMA, null, fileFormat,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"), temp);
    Table table = v3Helper.createUnpartitionedTable();
    DataFile dataFile = v3Helper.writeFile(null, v3Helper.generateRandomRecords(10, 0L));
    v3Helper.appendToTable(dataFile);
    DeleteFile dv = FileHelpers.writeDeleteFile(table, null, null,
        ImmutableList.of(Pair.of(dataFile.location(), 0L), Pair.of(dataFile.location(), 3L)), 3).first();
    table.newRowDelta().addDeletes(dv).commit();
    new InputFormatConfig.ConfigBuilder(job).readFrom(location.toString());
    job.setBoolean(HiveConf.ConfVars.LLAP_IO_ENABLED.varname, true);

    JobConf daemonConf = new JobConf(job);
    job.setBoolean(HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE.varname, false);

    List<LlapBufferOrBuffers> handedOut = Lists.newArrayList();
    MetadataCache cache = TestCachingDeleteLoader.trackingMetadataCache(job, handedOut);
    HiveIcebergInputFormat inputFormat = new HiveIcebergInputFormat();
    inputFormat.injectCaches(cache, null, daemonConf);

    TezCounters counters = new TezCounters();
    Assert.assertEquals(8, readRows(inputFormat, job, new MRReporter(counters)));
    Assert.assertEquals(8, readRows(inputFormat, job, Reporter.NULL));
    Assert.assertEquals(8, readRows(inputFormat, job, new MRReporter(counters)));
    TestCachingDeleteLoader.assertCounters(counters, 1, 1, 1, 0);

    ArgumentCaptor<CacheTag> tag = ArgumentCaptor.forClass(CacheTag.class);
    Mockito.verify(cache, times(3)).getFileMetadata(any());
    Mockito.verify(cache, times(1)).putFileMetadata(any(), any(ByteBuffer.class), tag.capture(), any());
    Assert.assertNotNull(tag.getValue());
    TestCachingDeleteLoader.assertReleased(handedOut);

    job.setBoolean(HiveConf.ConfVars.LLAP_IO_ENABLED.varname, false);
    Mockito.clearInvocations(cache);
    Assert.assertEquals(8, readRows(inputFormat, job, Reporter.NULL));
    Mockito.verifyNoInteractions(cache);
  }

  private static int readRows(HiveIcebergInputFormat inputFormat, JobConf job, Reporter reporter) throws IOException {
    int rows = 0;
    for (org.apache.hadoop.mapred.InputSplit split : inputFormat.getSplits(job, 1)) {
      org.apache.hadoop.mapred.RecordReader<Void, VectorizedRowBatch> reader =
          (org.apache.hadoop.mapred.RecordReader) inputFormat.getRecordReader(split, job, reporter);
      VectorizedRowBatch batch = reader.createValue();
      while (reader.next(null, batch)) {
        rows += batch.size;
      }
      reader.close();
    }
    return rows;
  }

}
