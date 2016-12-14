/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.druid.DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME;

public class DruidOutputFormat<K, V> implements HiveOutputFormat<K, DruidWritable> {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidOutputFormat.class);

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
          JobConf jc,
          Path finalOutPath,
          Class<? extends Writable> valueClass,
          boolean isCompressed,
          Properties tableProperties,
          Progressable progress
  ) throws IOException {

    final String segmentGranularity =
            tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) != null ?
                    tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) :
                    HiveConf.getVar(jc, HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY);
    final String dataSource = tableProperties.getProperty(Constants.DRUID_DATA_SOURCE);
    final String segmentDirectory =
            tableProperties.getProperty(Constants.DRUID_SEGMENT_DIRECTORY) != null
                    ? tableProperties.getProperty(Constants.DRUID_SEGMENT_DIRECTORY)
                    : HiveConf.getVar(jc, HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY);

    final HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    final DataSegmentPusher hdfsDataSegmentPusher = new HdfsDataSegmentPusher(
            hdfsDataSegmentPusherConfig, jc, DruidStorageHandlerUtils.JSON_MAPPER);

    final GranularitySpec granularitySpec = new UniformGranularitySpec(
            Granularity.valueOf(segmentGranularity),
            null,
            null
    );

    final String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    if (StringUtils.isEmpty(columnNameProperty) || StringUtils.isEmpty(columnTypeProperty)) {
      throw new IllegalStateException(
              String.format("List of columns names [%s] or columns type [%s] is/are not present",
                      columnNameProperty, columnTypeProperty
              ));
    }
    ArrayList<String> columnNames = new ArrayList<String>();
    for (String name : columnNameProperty.split(",")) {
      columnNames.add(name);
    }
    if (!columnNames.contains(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
      throw new IllegalStateException("Timestamp column (' " + DruidTable.DEFAULT_TIMESTAMP_COLUMN +
              "') not specified in create table; list of columns is : " +
              tableProperties.getProperty(serdeConstants.LIST_COLUMNS));
    }
    ArrayList<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    // Default, all columns that are not metrics or timestamp, are treated as dimensions
    final List<DimensionSchema> dimensions = new ArrayList<>();
    ImmutableList.Builder<AggregatorFactory> aggregatorFactoryBuilder = ImmutableList.builder();
    for (int i = 0; i < columnTypes.size(); i++) {
      TypeInfo f = columnTypes.get(i);
      assert f.getCategory() == ObjectInspector.Category.PRIMITIVE;
      AggregatorFactory af;
      switch (f.getTypeName()) {
        case serdeConstants.TINYINT_TYPE_NAME:
        case serdeConstants.SMALLINT_TYPE_NAME:
        case serdeConstants.INT_TYPE_NAME:
        case serdeConstants.BIGINT_TYPE_NAME:
          af = new LongSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
          break;
        case serdeConstants.FLOAT_TYPE_NAME:
        case serdeConstants.DOUBLE_TYPE_NAME:
          af = new DoubleSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
          break;
        default:
          // Dimension or timestamp
          String columnName = columnNames.get(i);
          if (!columnName.equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN) && !columnName
                  .equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME)) {
            dimensions.add(new StringDimensionSchema(columnName));
          }
          continue;
      }
      aggregatorFactoryBuilder.add(af);
    }
    List<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();
    final InputRowParser inputRowParser = new MapInputRowParser(new TimeAndDimsParseSpec(
            new TimestampSpec(DruidTable.DEFAULT_TIMESTAMP_COLUMN, "auto", null),
            new DimensionsSpec(dimensions,
                    Lists.newArrayList(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME), null
            )
    ));

    Map<String, Object> inputParser = DruidStorageHandlerUtils.JSON_MAPPER
            .convertValue(inputRowParser, Map.class);

    final DataSchema dataSchema = new DataSchema(
            Preconditions.checkNotNull(dataSource, "Data source name is null"),
            inputParser,
            aggregatorFactories.toArray(new AggregatorFactory[aggregatorFactories.size()]),
            granularitySpec,
            DruidStorageHandlerUtils.JSON_MAPPER
    );

    final String workingPath = jc.get(Constants.DRUID_JOB_WORKING_DIRECTORY);
    final String version = jc.get(Constants.DRUID_SEGMENT_VERSION);
    Integer maxPartitionSize = HiveConf
            .getIntVar(jc, HiveConf.ConfVars.HIVE_DRUID_MAX_PARTITION_SIZE);
    String basePersistDirectory = HiveConf
            .getVar(jc, HiveConf.ConfVars.HIVE_DRUID_BASE_PERSIST_DIRECTORY);
    final RealtimeTuningConfig realtimeTuningConfig = RealtimeTuningConfig
            .makeDefaultTuningConfig(new File(
                    basePersistDirectory))
            .withVersioningPolicy(new CustomVersioningPolicy(version));

    LOG.debug(String.format("running with Data schema [%s] ", dataSchema));
    return new DruidRecordWriter(dataSchema, realtimeTuningConfig, hdfsDataSegmentPusher,
            maxPartitionSize, new Path(workingPath, SEGMENTS_DESCRIPTOR_DIR_NAME),
            finalOutPath.getFileSystem(jc)
    );
  }

  @Override
  public RecordWriter<K, DruidWritable> getRecordWriter(
          FileSystem ignored, JobConf job, String name, Progressable progress
  ) throws IOException {
    throw new UnsupportedOperationException("please implement me !");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
