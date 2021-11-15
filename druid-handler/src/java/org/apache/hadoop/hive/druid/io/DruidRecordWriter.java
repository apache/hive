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
package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentNotWritableException;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Druid Record Writer, implementing a file sink operator.
 */
public class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>, FileSinkOperator.RecordWriter {
  protected static final Logger LOG = LoggerFactory.getLogger(DruidRecordWriter.class);

  private final DataSchema dataSchema;

  private final Appenderator appenderator;

  private final RealtimeTuningConfig tuningConfig;

  private final Path segmentsDescriptorDir;

  private SegmentIdWithShardSpec currentOpenSegment = null;

  private final int maxPartitionSize;

  private final FileSystem fileSystem;

  private final Supplier<Committer> committerSupplier;

  private final Granularity segmentGranularity;

  public DruidRecordWriter(DataSchema dataSchema,
      RealtimeTuningConfig realtimeTuningConfig,
      DataSegmentPusher dataSegmentPusher,
      int maxPartitionSize,
      final Path segmentsDescriptorsDir,
      final FileSystem fileSystem) {
    File basePersistDir = new File(realtimeTuningConfig.getBasePersistDirectory(), UUID.randomUUID().toString());
    this.tuningConfig =
        Preconditions.checkNotNull(realtimeTuningConfig.withBasePersistDirectory(basePersistDir),
            "realtimeTuningConfig is null");
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "data schema is null");

    appenderator = Appenderators
        .createOffline("hive-offline-appenderator", this.dataSchema, tuningConfig, false, new FireDepartmentMetrics(),
            dataSegmentPusher, DruidStorageHandlerUtils.JSON_MAPPER, DruidStorageHandlerUtils.INDEX_IO,
            DruidStorageHandlerUtils.INDEX_MERGER_V9);
    this.maxPartitionSize = maxPartitionSize;
    appenderator.startJob();
    this.segmentsDescriptorDir = Preconditions.checkNotNull(segmentsDescriptorsDir, "segmentsDescriptorsDir is null");
    this.fileSystem = Preconditions.checkNotNull(fileSystem, "file system is null");
    this.segmentGranularity = this.dataSchema.getGranularitySpec().getSegmentGranularity();
    committerSupplier = Suppliers.ofInstance(Committers.nil())::get;
  }

  /**
   * This function computes the segment identifier and push the current open segment
   * The push will occur if max size is reached or the event belongs to the next interval.
   * Note that this function assumes that timestamps are pseudo sorted.
   * This function will close and move to the next segment granularity as soon as
   * an event from the next interval appears. The sorting is done by the previous stage.
   *
   * @return segmentIdentifier with of the truncatedTime and maybe push the current open segment.
   */
  private SegmentIdWithShardSpec getSegmentIdentifierAndMaybePush(long truncatedTime) {

    DateTime truncatedDateTime = segmentGranularity.bucketStart(DateTimes.utc(truncatedTime));
    final Interval interval = new Interval(truncatedDateTime, segmentGranularity.increment(truncatedDateTime));

    SegmentIdWithShardSpec retVal;
    if (currentOpenSegment == null) {
      currentOpenSegment =
          new SegmentIdWithShardSpec(dataSchema.getDataSource(),
              interval,
              tuningConfig.getVersioningPolicy().getVersion(interval),
              new LinearShardSpec(0));
      return currentOpenSegment;
    } else if (currentOpenSegment.getInterval().equals(interval)) {
      retVal = currentOpenSegment;
      int rowCount = appenderator.getRowCount(retVal);
      if (rowCount < maxPartitionSize) {
        return retVal;
      } else {
        retVal =
            new SegmentIdWithShardSpec(dataSchema.getDataSource(),
                interval,
                tuningConfig.getVersioningPolicy().getVersion(interval),
                new LinearShardSpec(currentOpenSegment.getShardSpec().getPartitionNum() + 1));
        pushSegments(Lists.newArrayList(currentOpenSegment));
        LOG.info("Creating new partition for segment {}, partition num {}",
            retVal.toString(),
            retVal.getShardSpec().getPartitionNum());
        currentOpenSegment = retVal;
        return retVal;
      }
    } else {
      retVal =
          new SegmentIdWithShardSpec(dataSchema.getDataSource(),
              interval,
              tuningConfig.getVersioningPolicy().getVersion(interval),
              new LinearShardSpec(0));
      pushSegments(Lists.newArrayList(currentOpenSegment));
      LOG.info("Creating segment {}", retVal.toString());
      currentOpenSegment = retVal;
      return retVal;
    }
  }

  private void pushSegments(List<SegmentIdWithShardSpec> segmentsToPush) {
    try {
      SegmentsAndMetadata segmentsAndMetadata = appenderator.push(segmentsToPush, committerSupplier.get(), false).get();
      final Set<String> pushedSegmentIdentifierHashSet = new HashSet<>();

      for (DataSegment pushedSegment : segmentsAndMetadata.getSegments()) {
        pushedSegmentIdentifierHashSet.add(SegmentIdWithShardSpec.fromDataSegment(pushedSegment).toString());
        final Path
            segmentDescriptorOutputPath =
            DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(pushedSegment, segmentsDescriptorDir);
        DruidStorageHandlerUtils.writeSegmentDescriptor(fileSystem, pushedSegment, segmentDescriptorOutputPath);
        LOG.info(String.format("Pushed the segment [%s] and persisted the descriptor located at [%s]",
            pushedSegment,
            segmentDescriptorOutputPath));
      }

      final Set<String>
          toPushSegmentsHashSet =
          segmentsToPush.stream()
              .map(SegmentIdWithShardSpec::toString)
              .collect(Collectors.toCollection(HashSet::new));

      if (!pushedSegmentIdentifierHashSet.equals(toPushSegmentsHashSet)) {
        throw new IllegalStateException(String.format("was asked to publish [%s] but was able to publish only [%s]",
            Joiner.on(", ").join(toPushSegmentsHashSet),
            Joiner.on(", ").join(pushedSegmentIdentifierHashSet)));
      }
      for (SegmentIdWithShardSpec dataSegmentId : segmentsToPush) {
        LOG.info("Dropping segment {}", dataSegmentId.toString());
        appenderator.drop(dataSegmentId).get();
      }

      LOG.info(String.format("Published [%,d] segments.", segmentsToPush.size()));
    } catch (InterruptedException e) {
      LOG.error(String.format("got interrupted, failed to push  [%,d] segments.", segmentsToPush.size()), e);
      Thread.currentThread().interrupt();
    } catch (IOException | ExecutionException e) {
      LOG.error(String.format("Failed to push  [%,d] segments.", segmentsToPush.size()), e);
      Throwables.propagate(e);
    }
  }

  @Override public void write(Writable w) throws IOException {
    DruidWritable record = (DruidWritable) w;
    final long timestamp = (long) record.getValue().get(DruidConstants.DEFAULT_TIMESTAMP_COLUMN);
    final int
        partitionNumber =
        Math.toIntExact((long) record.getValue().getOrDefault(Constants.DRUID_SHARD_KEY_COL_NAME, -1L));
    final InputRow
        inputRow =
        new MapBasedInputRow(timestamp,
            dataSchema.getParser().getParseSpec().getDimensionsSpec().getDimensionNames(),
            record.getValue());

    try {

      if (partitionNumber != -1 && maxPartitionSize == -1) {
        /*
        Case data is sorted by time and an extra hashing dimension see DRUID_SHARD_KEY_COL_NAME
        Thus use DRUID_SHARD_KEY_COL_NAME as segment partition in addition to time dimension
        Data with the same DRUID_SHARD_KEY_COL_NAME and Time interval will end in the same segment
        */
        DateTime truncatedDateTime = segmentGranularity.bucketStart(DateTimes.utc(timestamp));
        final Interval interval = new Interval(truncatedDateTime, segmentGranularity.increment(truncatedDateTime));

        if (currentOpenSegment != null) {
          if (currentOpenSegment.getShardSpec().getPartitionNum() != partitionNumber
              || !currentOpenSegment.getInterval().equals(interval)) {
            pushSegments(ImmutableList.of(currentOpenSegment));
            currentOpenSegment =
                new SegmentIdWithShardSpec(dataSchema.getDataSource(),
                    interval,
                    tuningConfig.getVersioningPolicy().getVersion(interval),
                    new LinearShardSpec(partitionNumber));
          }
        } else {
          currentOpenSegment =
              new SegmentIdWithShardSpec(dataSchema.getDataSource(),
                  interval,
                  tuningConfig.getVersioningPolicy().getVersion(interval),
                  new LinearShardSpec(partitionNumber));

        }
        appenderator.add(currentOpenSegment, inputRow, committerSupplier::get);

      } else if (partitionNumber == -1 && maxPartitionSize != -1) {
        /*Case we are partitioning the segments based on time and max row per segment maxPartitionSize*/
        appenderator.add(getSegmentIdentifierAndMaybePush(timestamp), inputRow, committerSupplier::get);
      } else {
        throw new IllegalArgumentException(String.format(
            "partitionNumber and maxPartitionSize should be mutually exclusive "
                + "got partitionNum [%s] and maxPartitionSize [%s]",
            partitionNumber,
            maxPartitionSize));
      }

    } catch (SegmentNotWritableException e) {
      throw new IOException(e);
    }
  }

  @Override public void close(boolean abort) throws IOException {
    try {
      if (!abort) {
        final List<SegmentIdWithShardSpec> segmentsToPush = Lists.newArrayList();
        segmentsToPush.addAll(appenderator.getSegments());
        pushSegments(segmentsToPush);
      }
      appenderator.clear();
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    } finally {
      try {
        FileUtils.deleteDirectory(tuningConfig.getBasePersistDirectory());
      } catch (Exception e) {
        LOG.error("error cleaning of base persist directory", e);
      }
      appenderator.close();
    }
  }

  @Override public void write(NullWritable key, DruidWritable value) throws IOException {
    this.write(value);
  }

  @Override public void close(Reporter reporter) throws IOException {
    this.close(false);
  }

}
