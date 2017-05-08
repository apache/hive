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
package org.apache.hadoop.hive.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.metamx.common.MapUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.NoopEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.mysql.MySQLConnector;
import io.druid.query.BaseQuery;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.column.ColumnConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.StringUtils;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.hadoop.hive.ql.exec.Utilities.jarFinderGetJar;

/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandlerUtils.class);

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

  private static final int DEFAULT_STREAMING_RESULT_SIZE = 100;

  /**
   * Used by druid to perform IO on indexes
   */
  public static final IndexIO INDEX_IO = new IndexIO(JSON_MAPPER, new ColumnConfig() {
    @Override
    public int columnCacheSizeBytes() {
      return 0;
    }
  });

  /**
   * Used by druid to merge indexes
   */
  public static final IndexMergerV9 INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER,
          DruidStorageHandlerUtils.INDEX_IO
  );

  /**
   * Generic Interner implementation used to read segments object from metadata storage
   */
  public static final Interner<DataSegment> DATA_SEGMENT_INTERNER = Interners.newWeakInterner();

  static {
    // Register the shard sub type to be used by the mapper
    JSON_MAPPER.registerSubtypes(new NamedType(LinearShardSpec.class, "linear"));
    // set the timezone of the object mapper
    // THIS IS NOT WORKING workaround is to set it as part of java opts -Duser.timezone="UTC"
    JSON_MAPPER.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      // No operation emitter will be used by some internal druid classes.
      EmittingLogger.registerEmitter(
              new ServiceEmitter("druid-hive-indexer", InetAddress.getLocalHost().getHostName(),
                      new NoopEmitter()
              ));
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Method that creates a request for Druid JSON query (using SMILE).
   *
   * @param address
   * @param query
   *
   * @return
   *
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
   *
   * @param client
   * @param request
   *
   * @return
   *
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

  public static String getURL(HttpClient client, URL url) throws IOException {
    try (Reader reader = new InputStreamReader(
            DruidStorageHandlerUtils.submitRequest(client, new Request(HttpMethod.GET, url)))) {
      return CharStreams.toString(reader);
    }
  }

  /**
   * @param taskDir path to the  directory containing the segments descriptor info
   *                the descriptor path will be .../workingPath/task_id/{@link DruidStorageHandler#SEGMENTS_DESCRIPTOR_DIR_NAME}/*.json
   * @param conf    hadoop conf to get the file system
   *
   * @return List of DataSegments
   *
   * @throws IOException can be for the case we did not produce data.
   */
  public static List<DataSegment> getPublishedSegments(Path taskDir, Configuration conf)
          throws IOException {
    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();
    FileSystem fs = taskDir.getFileSystem(conf);
    for (FileStatus fileStatus : fs.listStatus(taskDir)) {
      final DataSegment segment = JSON_MAPPER
              .readValue(fs.open(fileStatus.getPath()), DataSegment.class);
      publishedSegmentsBuilder.add(segment);
    }
    List<DataSegment> publishedSegments = publishedSegmentsBuilder.build();
    return publishedSegments;
  }

  /**
   * This function will write to filesystem serialized from of segment descriptor
   * if an existing file exists it will try to replace it.
   *
   * @param outputFS       filesystem
   * @param segment        DataSegment object
   * @param descriptorPath path
   *
   * @throws IOException
   */
  public static void writeSegmentDescriptor(
          final FileSystem outputFS,
          final DataSegment segment,
          final Path descriptorPath
  )
          throws IOException {
    final DataPusher descriptorPusher = (DataPusher) RetryProxy.create(
            DataPusher.class, new DataPusher() {
              @Override
              public long push() throws IOException {
                try {
                  if (outputFS.exists(descriptorPath)) {
                    if (!outputFS.delete(descriptorPath, false)) {
                      throw new IOException(
                              String.format("Failed to delete descriptor at [%s]", descriptorPath));
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
                } catch (RuntimeException | IOException ex) {
                  throw ex;
                }
                return -1;
              }
            },
            RetryPolicies
                    .exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    descriptorPusher.push();
  }

  /**
   * @param connector                   SQL metadata connector to the metadata storage
   * @param metadataStorageTablesConfig Table config
   *
   * @return all the active data sources in the metadata storage
   */
  public static Collection<String> getAllDataSourceNames(SQLMetadataConnector connector,
          final MetadataStorageTablesConfig metadataStorageTablesConfig
  ) {
    return connector.getDBI().withHandle(
            new HandleCallback<List<String>>() {
              @Override
              public List<String> withHandle(Handle handle) throws Exception {
                return handle.createQuery(
                        String.format("SELECT DISTINCT(datasource) FROM %s WHERE used = true",
                                metadataStorageTablesConfig.getSegmentsTable()
                        ))
                        .fold(Lists.<String>newArrayList(),
                                new Folder3<ArrayList<String>, Map<String, Object>>() {
                                  @Override
                                  public ArrayList<String> fold(ArrayList<String> druidDataSources,
                                          Map<String, Object> stringObjectMap,
                                          FoldController foldController,
                                          StatementContext statementContext
                                  ) throws SQLException {
                                    druidDataSources.add(
                                            MapUtils.getString(stringObjectMap, "datasource")
                                    );
                                    return druidDataSources;
                                  }
                                }
                        );

              }
            }
    );
  }

  /**
   * @param connector                   SQL connector to metadata
   * @param metadataStorageTablesConfig Tables configuration
   * @param dataSource                  Name of data source
   *
   * @return true if the data source was successfully disabled false otherwise
   */
  public static boolean disableDataSource(SQLMetadataConnector connector,
          final MetadataStorageTablesConfig metadataStorageTablesConfig, final String dataSource
  ) {
    try {
      if (!getAllDataSourceNames(connector, metadataStorageTablesConfig).contains(dataSource)) {
        DruidStorageHandler.LOG
                .warn(String.format("Cannot delete data source [%s], does not exist", dataSource));
        return false;
      }

      connector.getDBI().withHandle(
              new HandleCallback<Void>() {
                @Override
                public Void withHandle(Handle handle) throws Exception {
                  handle.createStatement(
                          String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource",
                                  metadataStorageTablesConfig.getSegmentsTable()
                          )
                  )
                          .bind("dataSource", dataSource)
                          .execute();

                  return null;
                }
              }
      );

    } catch (Exception e) {
      DruidStorageHandler.LOG.error(String.format("Error removing dataSource %s", dataSource), e);
      return false;
    }
    return true;
  }

  /**
   * @param connector                   SQL connector to metadata
   * @param metadataStorageTablesConfig Tables configuration
   * @param dataSource                  Name of data source
   *
   * @return List of all data segments part of the given data source
   */
  public static List<DataSegment> getDataSegmentList(final SQLMetadataConnector connector,
          final MetadataStorageTablesConfig metadataStorageTablesConfig, final String dataSource
  ) {
    List<DataSegment> segmentList = connector.retryTransaction(
            new TransactionCallback<List<DataSegment>>() {
              @Override
              public List<DataSegment> inTransaction(
                      Handle handle, TransactionStatus status
              ) throws Exception {
                return handle
                        .createQuery(String.format(
                                "SELECT payload FROM %s WHERE dataSource = :dataSource",
                                metadataStorageTablesConfig.getSegmentsTable()
                        ))
                        .setFetchSize(getStreamingFetchSize(connector))
                        .bind("dataSource", dataSource)
                        .map(ByteArrayMapper.FIRST)
                        .fold(
                                new ArrayList<DataSegment>(),
                                new Folder3<List<DataSegment>, byte[]>() {
                                  @Override
                                  public List<DataSegment> fold(List<DataSegment> accumulator,
                                          byte[] payload, FoldController control,
                                          StatementContext ctx
                                  ) throws SQLException {
                                    try {
                                      final DataSegment segment = DATA_SEGMENT_INTERNER.intern(
                                              JSON_MAPPER.readValue(
                                                      payload,
                                                      DataSegment.class
                                              ));

                                      accumulator.add(segment);
                                      return accumulator;
                                    } catch (Exception e) {
                                      throw new SQLException(e.toString());
                                    }
                                  }
                                }
                        );
              }
            }
            , 3, SQLMetadataConnector.DEFAULT_MAX_TRIES);
    return segmentList;
  }

  /**
   * @param connector
   *
   * @return streaming fetch size.
   */
  private static int getStreamingFetchSize(SQLMetadataConnector connector) {
    if (connector instanceof MySQLConnector) {
      return Integer.MIN_VALUE;
    }
    return DEFAULT_STREAMING_RESULT_SIZE;
  }

  /**
   * @param pushedSegment
   * @param segmentsDescriptorDir
   *
   * @return a sanitize file name
   */
  public static Path makeSegmentDescriptorOutputPath(DataSegment pushedSegment,
          Path segmentsDescriptorDir
  ) {
    return new Path(
            segmentsDescriptorDir,
            String.format("%s.json", pushedSegment.getIdentifier().replace(":", ""))
    );
  }

  /**
   * Simple interface for retry operations
   */
  public interface DataPusher {
    long push() throws IOException;
  }

  // Thanks, HBase Storage handler
  public static void addDependencyJars(Configuration conf, Class<?>... classes) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    jars.addAll(conf.getStringCollection("tmpjars"));
    for (Class<?> clazz : classes) {
      if (clazz == null) {
        continue;
      }
      String path = Utilities.jarFinderGetJar(clazz);
      if (path == null) {
        throw new RuntimeException(
                "Could not find jar for class " + clazz + " in order to ship it to the cluster.");
      }
      if (!localFs.exists(new Path(path))) {
        throw new RuntimeException("Could not validate jar file " + path + " for class " + clazz);
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty()) {
      return;
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

}
