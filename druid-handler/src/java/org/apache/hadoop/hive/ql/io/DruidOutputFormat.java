package org.apache.hadoop.hive.ql.io;


import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.Granularity;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.DefaultOfflineAppenderatorFactory;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DruidOutputFormat<K, V> implements HiveOutputFormat<K, DruidWritable>
{
  private static final String DRUID_MAX_PARTITION_SIZE = "druid.maxPartitionSize";
  private static final Integer DEFAULT_MAX_ROW_IN_MEMORY = 75000;
  private final HiveConf hiveConf = SessionState.get().getConf();
  private static final int DEFAULT_MAX_PARTITION_SIZE = 5000000;

  public static class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>,
      org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter
  {
    protected static final Logger log = LoggerFactory.getLogger(DruidRecordWriter.class);

    private final DataSchema dataSchema;
    private final Appenderator appenderator;
    private final RealtimeTuningConfig tuningConfig;
    private SegmentIdentifier currentOpenSegment = null;
    private final Integer maxPartitionSize;
    private final Path segmentDescriptorDir;
    private final FileSystem fileSystem;
    private final Supplier<Committer> committerSupplier;

    public DruidRecordWriter(
        DataSchema dataSchema,
        RealtimeTuningConfig realtimeTuningConfig,
        Integer maxPartitionSize,
        final Path segmentDescriptorDir,
        final FileSystem fileSystem
    )
    {

      DefaultOfflineAppenderatorFactory defaultOfflineAppenderatorFactory = new DefaultOfflineAppenderatorFactory(
          DruidOutputFormatUtils.injector.getInstance(DataSegmentPusher.class),
          DruidStorageHandlerUtils.JSON_MAPPER,
          DruidOutputFormatUtils.INDEX_IO,
          DruidOutputFormatUtils.INDEX_MERGER
      );
      this.tuningConfig = realtimeTuningConfig;
      this.dataSchema = dataSchema;
      appenderator = defaultOfflineAppenderatorFactory.build(
          this.dataSchema,
          tuningConfig,
          new FireDepartmentMetrics()
      );
      this.maxPartitionSize = maxPartitionSize == null ? DEFAULT_MAX_PARTITION_SIZE : maxPartitionSize;

      appenderator.startJob(); // maybe we need to move this out of the constructor
      this.segmentDescriptorDir = Preconditions.checkNotNull(segmentDescriptorDir);
      this.fileSystem = Preconditions.checkNotNull(fileSystem);
      committerSupplier = Suppliers.ofInstance(Committers.nil());
    }

    /**
     * This function compute the segment identifier and push the current open segment if max size is reached or the event belongs to the next interval.
     * Note that this function assumes that timestamps are pseudo sorted.
     * This function will close and move to the next segment granularity as soon as it we get an event from the next interval.
     *
     * @param timestamp event timestamp as long
     *
     * @return segmentIdentifier with respect to the timestamp and maybe push the current open segment.
     */
    private SegmentIdentifier getSegmentIdentifierAndMaybePush(long timestamp)
    {
      final Granularity segmentGranularity;
      segmentGranularity = dataSchema.getGranularitySpec().getSegmentGranularity();
      final long truncatedTime = segmentGranularity.truncate(new DateTime(timestamp)).getMillis();

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

    private void pushSegments(List<SegmentIdentifier> segmentsToPush)
    {
      try {
        SegmentsAndMetadata segmentsAndMetadata = appenderator.push(segmentsToPush, committerSupplier.get()).get();
        final HashSet<String> pushedSegmentIdentifierHashSet = new HashSet<>();
        for (DataSegment pushedSegment : segmentsAndMetadata.getSegments()) {
          pushedSegmentIdentifierHashSet.add(SegmentIdentifier.fromDataSegment(pushedSegment).getIdentifierAsString());
          final Path segmentOutputPath = makeOutputPath(pushedSegment);
          DruidOutputFormatUtils.writeSegmentDescriptor(fileSystem, pushedSegment, segmentOutputPath);
          log.info(
              String.format("Pushed the segment [%s] and persisted the descriptor located at [%s]",
              pushedSegment,
              segmentOutputPath)
          );
        }

        final HashSet<String> toPushSegmentsHashSet = new HashSet(FluentIterable.from(segmentsToPush)
                                                                                .transform(new Function<SegmentIdentifier, String>()
                                                                                {
                                                                                  @Nullable
                                                                                  @Override
                                                                                  public String apply(
                                                                                      @Nullable SegmentIdentifier input
                                                                                  )
                                                                                  {
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
        log.info(String.format("Published [%,d] segments.", segmentsToPush.size()));
      }
      catch (InterruptedException e) {
        log.error(String.format("got interrupted, failed to push  [%,d] segments.", segmentsToPush.size()), e);
        Thread.currentThread().interrupt();
      }
      catch (IOException | ExecutionException e) {
        log.error(String.format("Failed to push  [%,d] segments.", segmentsToPush.size()), e);
        Throwables.propagate(e);
      }
    }


    @Override
    public void write(Writable w) throws IOException
    {
      if (w == null) {
        return;
      }
      DruidWritable record = (DruidWritable) w;
      final long timestamp = Longs.tryParse((String) record.getValue().get(DruidTable.DEFAULT_TIMESTAMP_COLUMN));
      InputRow inputRow = new MapBasedInputRow(
          timestamp,
          dataSchema.getParser()
                    .getParseSpec()
                    .getDimensionsSpec()
                    .getDimensionNames(),
          record.getValue()
      );

      try {
        appenderator.add(getSegmentIdentifierAndMaybePush(timestamp), inputRow, committerSupplier);
      }
      catch (SegmentNotWritableException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(boolean abort) throws IOException
    {
      try {
        if (abort == false) {
          final List<SegmentIdentifier> segmentsToPush = Lists.newArrayList();
          segmentsToPush.addAll(appenderator.getSegments());
          pushSegments(segmentsToPush);
        }
        appenderator.clear();
      }
      catch (InterruptedException e) {
        Throwables.propagate(e);
      }
      finally {
        appenderator.close();
      }
    }


    @Override
    public void write(NullWritable key, DruidWritable value) throws IOException
    {
      this.write(value);
    }

    @Override
    public void close(Reporter reporter) throws IOException
    {
      this.close(true);
    }

    private Path makeOutputPath(DataSegment pushedSegment)
    {
      return new Path(segmentDescriptorDir, String.format("%s.json", pushedSegment.getIdentifier().replace(":", "")));
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
  ) throws IOException
  {
    String tableSegmentGranularity = tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY);
    final String segmentGranularity = tableSegmentGranularity == null
                                      ? hiveConf.getVar(HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY)
                                      : tableSegmentGranularity;
    final String dataSource = tableProperties.getProperty(Constants.DRUID_DATA_SOURCE);
    final InputRowParser inputRowParser = new MapInputRowParser(new TimeAndDimsParseSpec(
        new TimestampSpec(DruidTable.DEFAULT_TIMESTAMP_COLUMN, "auto", null),
        new DimensionsSpec(null, null, null)
    ));

    final AggregatorFactory[] aggregatorFactories = DruidStorageHandlerUtils.JSON_MAPPER.readValue(
        tableProperties.getProperty(Constants.DRUID_AGGREGATORS),
        AggregatorFactory[].class
    );

    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularity.valueOf(segmentGranularity),
        null,
        null
    );
    Map<String, Object> inputParser = DruidStorageHandlerUtils.JSON_MAPPER.convertValue(inputRowParser, Map.class);

    final DataSchema dataSchema = new DataSchema(
        dataSource,
        inputParser,
        aggregatorFactories,
        granularitySpec,
        DruidStorageHandlerUtils.JSON_MAPPER
    );

    // this can be initialized from the hive conf
    String basePersistDirectory = hiveConf.getVar(HiveConf.ConfVars.HIVE_DRUID_BASE_PERSIST_DIRECTORY);
    final RealtimeTuningConfig realtimeTuningConfig = RealtimeTuningConfig.makeDefaultTuningConfig(new File(basePersistDirectory)).withVersioningPolicy(new CustomVersioningPolicy(null));
    Integer maxPartitionSize = hiveConf.getInt(DRUID_MAX_PARTITION_SIZE, DEFAULT_MAX_PARTITION_SIZE);
    return new DruidRecordWriter(
        dataSchema,
        realtimeTuningConfig,
        maxPartitionSize,
        finalOutPath,
        finalOutPath.getFileSystem(jc)
    );
  }

  @Override
  public RecordWriter<K, DruidWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress
  ) throws IOException
  {
    throw new UnsupportedOperationException("please implement me !");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
