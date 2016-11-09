/**
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
package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.Granularity;
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
  public static class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>,
          org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {
    protected static final Logger LOG = LoggerFactory.getLogger(DruidRecordWriter.class);

    private final DataSchema dataSchema;

    private final Appenderator appenderator;

    private final RealtimeTuningConfig tuningConfig;

    private final Path segmentsDescriptorDir;

    private SegmentIdentifier currentOpenSegment = null;

    private final Integer maxPartitionSize;

    private final FileSystem fileSystem;

    private final Supplier<Committer> committerSupplier;

    public DruidRecordWriter(
            DataSchema dataSchema,
            RealtimeTuningConfig realtimeTuningConfig,
            DataSegmentPusher dataSegmentPusher,
            Integer maxPartitionSize,
            final Path segmentsDescriptorsDir,
            final FileSystem fileSystem
    ) {
      DefaultOfflineAppenderatorFactory defaultOfflineAppenderatorFactory = new DefaultOfflineAppenderatorFactory(
              Preconditions.checkNotNull(dataSegmentPusher),
              DruidStorageHandlerUtils.JSON_MAPPER,
              DruidStorageHandlerUtils.INDEX_IO,
              DruidStorageHandlerUtils.INDEX_MERGER
      );
      this.tuningConfig = realtimeTuningConfig;
      this.dataSchema = dataSchema;
      appenderator = defaultOfflineAppenderatorFactory.build(
              this.dataSchema,
              tuningConfig,
              new FireDepartmentMetrics()
      );
      this.maxPartitionSize = maxPartitionSize;
      appenderator.startJob(); // maybe we need to move this out of the constructor
      this.segmentsDescriptorDir = segmentsDescriptorsDir;
      this.fileSystem = Preconditions.checkNotNull(fileSystem);
      committerSupplier = Suppliers.ofInstance(Committers.nil());
    }

    /**
     * This function compute the segment identifier and push the current open segment if max size is reached or the event belongs to the next interval.
     * Note that this function assumes that timestamps are pseudo sorted.
     * This function will close and move to the next segment granularity as soon as it we get an event from the next interval.
     *
     * @return segmentIdentifier with respect to the timestamp and maybe push the current open segment.
     */
    private SegmentIdentifier getSegmentIdentifierAndMaybePush(long truncatedTime) {

      final Granularity segmentGranularity = dataSchema.getGranularitySpec()
              .getSegmentGranularity();

      final Interval interval = new Interval(
              new DateTime(truncatedTime),
              segmentGranularity.increment(new DateTime(truncatedTime))
      );

      SegmentIdentifier retVal;
      if (currentOpenSegment == null) {
        retVal = new SegmentIdentifier(
                dataSchema.getDataSource(),
                interval,
                tuningConfig.getVersioningPolicy().getVersion(interval),
                new LinearShardSpec(0)
        );
        currentOpenSegment = retVal;
        return retVal;
      } else if (currentOpenSegment.getInterval().equals(interval)) {
        retVal = currentOpenSegment;
        int rowCount = appenderator.getRowCount(retVal);
        if (rowCount < maxPartitionSize) {
          return retVal;
        } else {
          retVal = new SegmentIdentifier(
                  dataSchema.getDataSource(),
                  interval,
                  tuningConfig.getVersioningPolicy().getVersion(interval),
                  new LinearShardSpec(currentOpenSegment.getShardSpec().getPartitionNum() + 1)
          );
          pushSegments(Lists.newArrayList(currentOpenSegment));
          currentOpenSegment = retVal;
          return retVal;
        }
      } else {
        retVal = new SegmentIdentifier(
                dataSchema.getDataSource(),
                interval,
                tuningConfig.getVersioningPolicy().getVersion(interval),
                new LinearShardSpec(0)
        );
        pushSegments(Lists.newArrayList(currentOpenSegment));
        currentOpenSegment = retVal;
        return retVal;
      }
    }

    private void pushSegments(List<SegmentIdentifier> segmentsToPush) {
      try {
        SegmentsAndMetadata segmentsAndMetadata = appenderator
                .push(segmentsToPush, committerSupplier.get()).get();
        final HashSet<String> pushedSegmentIdentifierHashSet = new HashSet<>();

        for (DataSegment pushedSegment : segmentsAndMetadata.getSegments()) {
          pushedSegmentIdentifierHashSet
                  .add(SegmentIdentifier.fromDataSegment(pushedSegment).getIdentifierAsString());
          final Path segmentDescriptorOutputPath = makeSegmentDescriptorOutputPath(pushedSegment);
          DruidStorageHandlerUtils
                  .writeSegmentDescriptor(fileSystem, pushedSegment, segmentDescriptorOutputPath);

          LOG.info(
                  String.format(
                          "Pushed the segment [%s] and persisted the descriptor located at [%s]",
                          pushedSegment,
                          segmentDescriptorOutputPath
                  )
          );
        }

        final HashSet<String> toPushSegmentsHashSet = new HashSet(
                FluentIterable.from(segmentsToPush)
                        .transform(new Function<SegmentIdentifier, String>() {
                          @Nullable
                          @Override
                          public String apply(
                                  @Nullable SegmentIdentifier input
                          ) {
                            return input.getIdentifierAsString();
                          }
                        })
                        .toList());

        if (!pushedSegmentIdentifierHashSet.equals(toPushSegmentsHashSet)) {
          throw new IllegalStateException(String.format(
                  "was asked to publish [%s] but was able to publish only [%s]",
                  Joiner.on(", ").join(toPushSegmentsHashSet),
                  Joiner.on(", ").join(pushedSegmentIdentifierHashSet)
          ));
        }
        LOG.info(String.format("Published [%,d] segments.", segmentsToPush.size()));
      } catch (InterruptedException e) {
        LOG.error(String.format("got interrupted, failed to push  [%,d] segments.",
                segmentsToPush.size()
        ), e);
        Thread.currentThread().interrupt();
      } catch (IOException | ExecutionException e) {
        LOG.error(String.format("Failed to push  [%,d] segments.", segmentsToPush.size()), e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void write(Writable w) throws IOException {
      if (w == null) {
        return;
      }
      DruidWritable record = (DruidWritable) w;
      final long timestamp = (long) record.getValue().get(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
      final long truncatedTime = (long) record.getValue()
              .get(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME);
      // We drop the time granularity column, since we do not need to store it
      Map event = Maps.filterKeys(record.getValue(), new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String input) {
          if (input.equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME)) {
            return false;
          }
          return true;
        }
      });

      InputRow inputRow = new MapBasedInputRow(
              timestamp,
              dataSchema.getParser()
                      .getParseSpec()
                      .getDimensionsSpec()
                      .getDimensionNames(),
              event
      );

      try {
        appenderator
                .add(getSegmentIdentifierAndMaybePush(truncatedTime), inputRow, committerSupplier);
      } catch (SegmentNotWritableException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(boolean abort) throws IOException {
      try {
        if (abort == false) {
          final List<SegmentIdentifier> segmentsToPush = Lists.newArrayList();
          segmentsToPush.addAll(appenderator.getSegments());
          pushSegments(segmentsToPush);
        }
        appenderator.clear();
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      } finally {
        appenderator.close();
      }
    }

    @Override
    public void write(NullWritable key, DruidWritable value) throws IOException {
      this.write(value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      this.close(true);
    }

    private Path makeSegmentDescriptorOutputPath(DataSegment pushedSegment) {
      return new Path(
              segmentsDescriptorDir,
              String.format("%s.json", pushedSegment.getIdentifier().replace(":", ""))
      );
    }

  }

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
          if (!columnName.equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
            dimensions.add(new StringDimensionSchema(columnName));
          }
          continue;
      }
      aggregatorFactoryBuilder.add(af);
    }
    List<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();
    final InputRowParser inputRowParser = new MapInputRowParser(new TimeAndDimsParseSpec(
            new TimestampSpec(DruidTable.DEFAULT_TIMESTAMP_COLUMN, "auto", null),
            new DimensionsSpec(dimensions, null, null)
    ));

    Map<String, Object> inputParser = DruidStorageHandlerUtils.JSON_MAPPER
            .convertValue(inputRowParser, Map.class);

    final DataSchema dataSchema = new DataSchema(
            Preconditions.checkNotNull(dataSource, "Data Source is null"),
            inputParser,
            aggregatorFactories.toArray(new AggregatorFactory[aggregatorFactories.size()]),
            granularitySpec,
            DruidStorageHandlerUtils.JSON_MAPPER
    );

    Integer maxPartitionSize = HiveConf
            .getIntVar(jc, HiveConf.ConfVars.HIVE_DRUID_MAX_PARTITION_SIZE);
    String basePersistDirectory = HiveConf
            .getVar(jc, HiveConf.ConfVars.HIVE_DRUID_BASE_PERSIST_DIRECTORY);
    final RealtimeTuningConfig realtimeTuningConfig = RealtimeTuningConfig
            .makeDefaultTuningConfig(new File(
                    basePersistDirectory)).withVersioningPolicy(new CustomVersioningPolicy(null));

    LOG.debug(String.format("running with DataSchema [%s] ", dataSchema));
    return new DruidRecordWriter(
            dataSchema,
            realtimeTuningConfig,
            hdfsDataSegmentPusher, maxPartitionSize,
            makeSegmentDescriptorOutputDir(finalOutPath),
            finalOutPath.getFileSystem(jc)
    );
  }

  private Path makeSegmentDescriptorOutputDir(Path finalOutPath) {
    return new Path(finalOutPath, SEGMENTS_DESCRIPTOR_DIR_NAME);
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
