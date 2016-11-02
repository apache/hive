package org.apache.hadoop.hive.ql.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;

public class DruidRecordWriterTest
{
  private ObjectMapper objectMapper = new DefaultObjectMapper();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private DruidOutputFormat.DruidRecordWriter druidRecordWriter;
  @Test
  public void testWrite() throws IOException
  {
    final Map<String, Object> parserMap = objectMapper.convertValue(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("__time", "auto", null),
                new DimensionsSpec(null, null, null),
                null,
                null
            )
        ),
        Map.class
    );
    DataSchema dataSchema = new DataSchema(
        "dataSourceName",
        parserMap,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("met", "met")
        },
        new UniformGranularitySpec(Granularity.MINUTE, QueryGranularities.NONE, null),
        objectMapper
    );

    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        75000,
        null,
        null,
        temporaryFolder.newFolder(),
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null
    );


    Configuration config = new Configuration();
    JobConf jobConf = new JobConf(config);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);


    druidRecordWriter = new DruidOutputFormat.DruidRecordWriter(dataSchema, tuningConfig, 20, new Path("/tmp/slim/test"), localFileSystem, jobConf);
    druidRecordWriter.write(null);
    DruidWritable druidWritable = new DruidWritable(ImmutableMap.<String, Object>of(
        DruidTable.DEFAULT_TIMESTAMP_COLUMN,
        new DateTime().getMillis(),
        Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME,
        Granularity.MINUTE.truncate(new DateTime()).getMillis(),
        "dim",
        "test",
        "met",
        "1"
    ));
    druidRecordWriter.write(druidWritable);
    druidRecordWriter.close(false);

  }

  @Test
  public void testSerDesr() throws IOException {
    String segment = "{\"dataSource\":\"datasource2015\",\"interval\":\"2015-06-01T00:00:00.000-04:00/2015-06-02T00:00:00.000-04:00\",\"version\":\"2016-11-04T19:24:01.732-04:00\",\"loadSpec\":{\"type\":\"hdfs\",\"path\":\"hdfs://cn105-10.l42scl.hortonworks.com:8020/apps/hive/warehouse/druid.db/.hive-staging_hive_2016-11-04_19-23-50_168_1550339856804207572-1/_task_tmp.-ext-10002/_tmp.000000_0/datasource2015/20150601T000000.000-0400_20150602T000000.000-0400/2016-11-04T19_24_01.732-04_00/0/index.zip\"},\"dimensions\":\"dimension1\",\"metrics\":\"bigint\",\"shardSpec\":{\"type\":\"LinearShardSpec\",\"partitionNum\":0},\"binaryVersion\":9,\"size\":1765,\"identifier\":\"datasource2015_2015-06-01T00:00:00.000-04:00_2015-06-02T00:00:00.000-04:00_2016-11-04T19:24:01.732-04:00\"}";
    DataSegment dataSegment = DruidStorageHandlerUtils.JSON_MAPPER.reader(DataSegment.class).readValue(segment);
    Assert.assertTrue(dataSegment != null);
  }

}