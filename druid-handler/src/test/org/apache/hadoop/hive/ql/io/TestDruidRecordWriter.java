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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.timeline.DataSegment;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.DruidStorageHandler;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.io.DruidRecordWriter;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test Class for Druid Record Writer.
 */
@SuppressWarnings("ConstantConditions") public class TestDruidRecordWriter {
  private final ObjectMapper objectMapper = DruidStorageHandlerUtils.JSON_MAPPER;

  private static final Interval INTERVAL_FULL = new Interval("2014-10-22T00:00:00Z/P1D");

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  final List<ImmutableMap<String, Object>>
      expectedRows =
      ImmutableList.of(ImmutableMap.of(DruidConstants.DEFAULT_TIMESTAMP_COLUMN,
          DateTime.parse("2014-10-22T00:00:00.000Z").getMillis(),
          "host",
          ImmutableList.of("a.example.com"),
          "visited_sum",
          190L,
          "unique_hosts",
          1.0d),
          ImmutableMap.of(DruidConstants.DEFAULT_TIMESTAMP_COLUMN,
              DateTime.parse("2014-10-22T01:00:00.000Z").getMillis(),
              "host",
              ImmutableList.of("b.example.com"),
              "visited_sum",
              175L,
              "unique_hosts",
              1.0d),
          ImmutableMap.of(DruidConstants.DEFAULT_TIMESTAMP_COLUMN,
              DateTime.parse("2014-10-22T02:00:00.000Z").getMillis(),
              "host",
              ImmutableList.of("c.example.com"),
              "visited_sum",
              270L,
              "unique_hosts",
              1.0d));

  @Test public void testTimeStampColumnName() {
    Assert.assertEquals("Time column name need to match to ensure serdeser compatibility",
        DruidConstants.DEFAULT_TIMESTAMP_COLUMN,
        DruidTable.DEFAULT_TIMESTAMP_COLUMN);
  }

  //Test is failing due to Guava dependency, Druid 0.13.0 should have less dependency on Guava
  @Ignore @Test public void testWrite() throws IOException, SegmentLoadingException {

    final String dataSourceName = "testDataSource";
    final File segmentOutputDir = temporaryFolder.newFolder();
    final File workingDir = temporaryFolder.newFolder();
    Configuration config = new Configuration();

    final InputRowParser
        inputRowParser =
        new MapInputRowParser(new TimeAndDimsParseSpec(new TimestampSpec(DruidConstants.DEFAULT_TIMESTAMP_COLUMN,
            "auto",
            null), new DimensionsSpec(ImmutableList.of(new StringDimensionSchema("host")), null, null)));
    final Map<String, Object>
        parserMap =
        objectMapper.convertValue(inputRowParser, new TypeReference<Map<String, Object>>() {
        });

    DataSchema
        dataSchema =
        new DataSchema(dataSourceName,
            parserMap,
            new AggregatorFactory[] {new LongSumAggregatorFactory("visited_sum", "visited_sum"),
                new HyperUniquesAggregatorFactory("unique_hosts", "unique_hosts") },
            new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, ImmutableList.of(INTERVAL_FULL)),
            null,
            objectMapper);

    IndexSpec indexSpec = new IndexSpec(new RoaringBitmapSerdeFactory(true), null, null, null);
    RealtimeTuningConfig
        tuningConfig =
        new RealtimeTuningConfig(null,
            null, null, null, temporaryFolder.newFolder(), null, null, null, null, indexSpec, null, null, 0, 0, null,
            null, 0L, null, null);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig() {
      @Override public File getStorageDirectory() {
        return segmentOutputDir;
      }
    });

    Path
        segmentDescriptorPath =
        new Path(workingDir.getAbsolutePath(), DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME);
    DruidRecordWriter
        druidRecordWriter =
        new DruidRecordWriter(dataSchema, tuningConfig, dataSegmentPusher, 20, segmentDescriptorPath, localFileSystem);

    List<DruidWritable>
        druidWritables =
        expectedRows.stream()
            .map(input -> new DruidWritable(ImmutableMap.<String, Object>builder().putAll(input)
                .put(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME,
                    Granularities
                        .DAY
                        .bucketStart(new DateTime((long) input.get(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)))
                        .getMillis())
                .build()))
            .collect(Collectors.toList());
    for (DruidWritable druidWritable : druidWritables) {
      druidRecordWriter.write(druidWritable);
    }
    druidRecordWriter.close(false);
    List<DataSegment> dataSegmentList = DruidStorageHandlerUtils.getCreatedSegments(segmentDescriptorPath, config);
    Assert.assertEquals(1, dataSegmentList.size());
    File tmpUnzippedSegmentDir = temporaryFolder.newFolder();
    new LocalDataSegmentPuller().getSegmentFiles(dataSegmentList.get(0), tmpUnzippedSegmentDir);
    final QueryableIndex queryableIndex = DruidStorageHandlerUtils.INDEX_IO.loadIndex(tmpUnzippedSegmentDir);

    QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(queryableIndex);

    Firehose
        firehose =
        new IngestSegmentFirehose(ImmutableList.of(new WindowedStorageAdapter(adapter, adapter.getInterval())),
            null,
            ImmutableList.of("host"),
            ImmutableList.of("visited_sum", "unique_hosts"),
            null);

    List<InputRow> rows = Lists.newArrayList();
    while (firehose.hasMore()) {
      rows.add(firehose.nextRow());
    }

    verifyRows(expectedRows, rows);

  }

  private void verifyRows(List<ImmutableMap<String, Object>> expectedRows, List<InputRow> actualRows) {
    System.out.println("actualRows = " + actualRows);
    Assert.assertEquals(expectedRows.size(), actualRows.size());

    for (int i = 0; i < expectedRows.size(); i++) {
      Map<String, Object> expected = expectedRows.get(i);
      InputRow actual = actualRows.get(i);

      Assert.assertEquals(ImmutableList.of("host"), actual.getDimensions());

      Assert.assertEquals(expected.get(DruidConstants.DEFAULT_TIMESTAMP_COLUMN),
          actual.getTimestamp().getMillis());
      Assert.assertEquals(expected.get("host"), actual.getDimension("host"));
      Assert.assertEquals(expected.get("visited_sum"), actual.getMetric("visited_sum"));
      Assert.assertEquals((Double) expected.get("unique_hosts"),
          (Double) HyperUniquesAggregatorFactory.estimateCardinality(actual.getRaw("unique_hosts"), false),
          0.001);
    }
  }

  @Test public void testSerDesr() throws IOException {
    String
        segment =
        "{\"dataSource\":\"datasource2015\",\"interval\":\"2015-06-01T00:00:00.000-04:00/"
            + "2015-06-02T00:00:00.000-04:00\""
            + ",\"version\":\"2016-11-04T19:24:01.732-04:00\",\"loadSpec\":{\"type\":\"hdfs\","
            + "\"path\":\"hdfs://cn105-10.l42scl.hortonworks.com:8020/apps/hive/warehouse/druid.db/"
            + ".hive-staging_hive_2016-11-04_19-23-50_168_1550339856804207572-1/_task_tmp.-ext-10002/_tmp.000000_0/"
            + "datasource2015/20150601T000000.000-0400_20150602T000000.000-0400/2016-11-04T19_24_01.732-04_00/0/"
            + "index.zip\"},\"dimensions\":\"dimension1\",\"metrics\":\"bigint\",\"shardSpec\":{\"type\":\"linear\","
            + "\"partitionNum\":0},\"binaryVersion\":9,\"size\":1765,\"identifier\":\"datasource2015_2015-06-01"
            + "T00:00:00.000-04:00_2015-06-02T00:00:00.000-04:00_2016-11-04T19:24:01.732-04:00\"}";
    DataSegment dataSegment = objectMapper.readerFor(DataSegment.class).readValue(segment);
    Assert.assertEquals("datasource2015", dataSegment.getDataSource());
  }

}
