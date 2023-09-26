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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat.CompatibilityTaskAttemptContextImpl;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.types.Types;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
        try (RecordReader<Void, ?> reader = inputFormat.createRecordReader(split, context)) {
          reader.initialize(split, context);
        }
      }
      mockedParquetFileReader.verify(times(1), () ->
          ParquetFileReader.readFooter(any(InputFile.class), any(ParquetMetadataConverter.MetadataFilter.class))
      );
    }
  }

}
