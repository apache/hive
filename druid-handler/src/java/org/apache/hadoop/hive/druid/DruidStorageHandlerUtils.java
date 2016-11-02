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
package org.apache.hadoop.hive.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.NoopEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BaseQuery;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.column.ColumnConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {




  private static final String SMILE_CONTENT_TYPE = "application/x-jackson-smile";

  /**
   * Mapper to use to serialize/deserialize Druid objects (JSON)
   */
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  /**
   * Mapper to use to serialize/deserialize Druid objects (SMILE)
   */
  public static final ObjectMapper SMILE_MAPPER = new DefaultObjectMapper(new SmileFactory());

  private static final int NUM_RETRIES = 8;

  private static final int SECONDS_BETWEEN_RETRIES = 2;

  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB

  public static final IndexIO INDEX_IO = new IndexIO(JSON_MAPPER, new ColumnConfig()
  {
    @Override
    public int columnCacheSizeBytes()
    {
      return 0;
    }
  });

  public static final IndexMerger INDEX_MERGER = new IndexMerger(JSON_MAPPER, DruidStorageHandlerUtils.INDEX_IO);
  public static final IndexMergerV9 INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, DruidStorageHandlerUtils.INDEX_IO);

  static {
    JSON_MAPPER.registerSubtypes(LinearShardSpec.class);
    try {
      EmittingLogger.registerEmitter(
              new ServiceEmitter("druid-hive-indexer", InetAddress.getLocalHost().getHostName(),
                      new NoopEmitter()
              ));
    } catch (UnknownHostException e) {
      Throwables.propagate(e);
    }
  }
  /**
   * Method that creates a request for Druid JSON query (using SMILE).
   * @param address
   * @param query
   * @return
   * @throws IOException
   */
  public static Request createRequest(String address, BaseQuery<?> query)
          throws IOException {
    return new Request(HttpMethod.POST, new URL(String.format("%s/druid/v2/", "http://" + address)))
            .setContent(SMILE_MAPPER.writeValueAsBytes(query))
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, SMILE_CONTENT_TYPE);
  }

  /**
   * Method that submits a request to an Http address and retrieves the result.
   * The caller is responsible for closing the stream once it finishes consuming it.
   * @param client
   * @param request
   * @return
   * @throws IOException
   */
  public static InputStream submitRequest(HttpClient client, Request request)
          throws IOException {
    InputStream response;
    try {
      response = client.go(request, new InputStreamResponseHandler()).get();
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } catch (InterruptedException e) {
      throw new IOException(e.getCause());
    }
    return response;
  }

  /**
   * @param tableDir path to the table directory containing the segments descriptor info
   *                 the descriptor path will be .../tableDir/task_id/{@link DruidStorageHandler#SEGMENTS_DESCRIPTOR_DIR_NAME}/*.json
   * @param conf     hadoop conf to get the file system
   *
   * @return List of DataSegments
   *
   * @throws IOException can be for the case we did not produce data.
   */

  public static List<DataSegment> getPublishedSegments(Path tableDir, Configuration conf) throws IOException
  {
    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();
    FileSystem fs = tableDir.getFileSystem(conf);
    for (FileStatus status : fs.listStatus(tableDir)) {
      Path taskDir = new Path(status.getPath(), DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME);
      if (!fs.isDirectory(taskDir)) {
        continue;
      }
      for (FileStatus fileStatus : fs.listStatus(taskDir))
      {
        final DataSegment segment = JSON_MAPPER.readValue(fs.open(fileStatus.getPath()), DataSegment.class);
        publishedSegmentsBuilder.add(segment);
      }
    }
    List<DataSegment> publishedSegments = publishedSegmentsBuilder.build();
    return publishedSegments;
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

  /**
   * Simple interface for retry operations
   */
  public interface DataPusher
  {
    long push() throws IOException;
  }
}
