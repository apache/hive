package org.apache.hadoop.hive.ql.io;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.Initialization;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.server.DruidNode;
import io.druid.storage.hdfs.HdfsStorageDruidModule;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DruidOutputFormatUtils
{
  public static final Injector injector;
  public static final ObjectMapper JSON_MAPPER;
  public static final IndexIO INDEX_IO;
  public static final IndexMerger INDEX_MERGER;
  public static final IndexMergerV9 INDEX_MERGER_V9;
  public static final ObjectMapper SMILE_MAPPER;
  private static final int NUM_RETRIES = 8;
  private static final int SECONDS_BETWEEN_RETRIES = 2;
  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB

  static {
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("hive-druid", null, null)
            );
          }
        }, new HdfsStorageDruidModule())
    );
    JSON_MAPPER = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    SMILE_MAPPER = injector.getInstance(Key.get(ObjectMapper.class, Smile.class));
    INDEX_IO = injector.getInstance(IndexIO.class);
    INDEX_MERGER = injector.getInstance(IndexMerger.class);
    INDEX_MERGER_V9 = injector.getInstance(IndexMergerV9.class);
  }

  /**
   * @param descriptorInfoDir path to the directory containing the segments descriptor info
   * @param conf hadoop conf to get the file system
   *
   * @return List of DataSegments
   *
   * @throws FileNotFoundException can be for the case we did not produce data.
   */

  public static List<DataSegment> getPublishedSegments(Path descriptorInfoDir, Configuration conf) throws IOException
  {
    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();

      FileSystem fs = descriptorInfoDir.getFileSystem(conf);
      for (FileStatus status : fs.listStatus(descriptorInfoDir)) {
        final DataSegment segment = JSON_MAPPER.readValue(fs.open(status.getPath()), DataSegment.class);
        publishedSegmentsBuilder.add(segment);
      }
    List<DataSegment> publishedSegments = publishedSegmentsBuilder.build();
    return publishedSegments;
  }

  /**
   * Simple interface for retry operations
   */
  public interface DataPusher
  {
    long push() throws IOException;
  }

  public static void writeSegmentDescriptor(
      final FileSystem outputFS,
      final DataSegment segment,
      final Path descriptorPath
  )
      throws IOException
  {
    final DataPusher descriptorPusher = (DataPusher) RetryProxy.create(
        DataPusher.class, new DataPusher()
        {
          @Override
          public long push() throws IOException
          {
            try {
              if (outputFS.exists(descriptorPath)) {
                if (!outputFS.delete(descriptorPath, false)) {
                  throw new IOException(String.format("Failed to delete descriptor at [%s]", descriptorPath));
                }
              }
              try (final OutputStream descriptorOut = outputFS.create(
                  descriptorPath,
                  true,
                  DEFAULT_FS_BUFFER_SIZE
              )) {
                JSON_MAPPER.writeValue(descriptorOut, segment);
                descriptorOut.flush();
              }
            }
            catch (RuntimeException | IOException ex) {
              throw ex;
            }
            return -1;
          }
        },
        RetryPolicies.exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    descriptorPusher.push();
  }

}
