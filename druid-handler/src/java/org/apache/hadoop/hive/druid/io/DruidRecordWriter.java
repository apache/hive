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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>,
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
          int maxPartitionSize,
          final Path segmentsDescriptorsDir,
          final FileSystem fileSystem
  ) {
    File basePersistDir = new File(realtimeTuningConfig.getBasePersistDirectory(),
            UUID.randomUUID().toString()
    );
    this.tuningConfig = Preconditions
            .checkNotNull(realtimeTuningConfig.withBasePersistDirectory(basePersistDir),
                    "realtimeTuningConfig is null"
            );
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "data schema is null");

    appenderator = Appenderators
            .createOffline(this.dataSchema, tuningConfig, new FireDepartmentMetrics(),
                    dataSegmentPusher, DruidStorageHandlerUtils.JSON_MAPPER,
                    DruidStorageHandlerUtils.INDEX_IO, DruidStorageHandlerUtils.INDEX_MERGER_V9
            );
    Preconditions.checkArgument(maxPartitionSize > 0, "maxPartitionSize need to be greater than 0");
    this.maxPartitionSize = maxPartitionSize;
    appenderator.startJob(); // maybe we need to move this out of the constructor
    this.segmentsDescriptorDir = Preconditions
            .checkNotNull(segmentsDescriptorsDir, "segmentsDescriptorsDir is null");
    this.fileSystem = Preconditions.checkNotNull(fileSystem, "file system is null");
    committerSupplier = Suppliers.ofInstance(Committers.nil());
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
        LOG.info("Creating new partition for segment {}, partition num {}",
                retVal.getIdentifierAsString(), retVal.getShardSpec().getPartitionNum());
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
      LOG.info("Creating segment {}", retVal.getIdentifierAsString());
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
        final Path segmentDescriptorOutputPath = DruidStorageHandlerUtils
                .makeSegmentDescriptorOutputPath(pushedSegment, segmentsDescriptorDir);
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
      for (SegmentIdentifier dataSegmentId : segmentsToPush) {
        LOG.info("Dropping segment {}", dataSegmentId.toString());
        appenderator.drop(dataSegmentId).get();
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
    DruidWritable record = (DruidWritable) w;
    final long timestamp = (long) record.getValue().get(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
    final long truncatedTime = (long) record.getValue()
            .get(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME);

    InputRow inputRow = new MapBasedInputRow(
            timestamp,
            dataSchema.getParser()
                    .getParseSpec()
                    .getDimensionsSpec()
                    .getDimensionNames(),
            record.getValue()
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
      try {
        FileUtils.deleteDirectory(tuningConfig.getBasePersistDirectory());
      } catch (Exception e){
        LOG.error("error cleaning of base persist directory", e);
      }
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

}
