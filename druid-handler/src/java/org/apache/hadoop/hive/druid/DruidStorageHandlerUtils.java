/*
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.CharStreams;
import com.metamx.common.JodaUtils;
import com.metamx.common.MapUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.NoopEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.math.expr.ExprMacroTable;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.mysql.MySQLConnector;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.expression.LikeExprMacro;
import io.druid.query.expression.RegexpExtractExprMacro;
import io.druid.query.expression.TimestampCeilExprMacro;
import io.druid.query.expression.TimestampExtractExprMacro;
import io.druid.query.expression.TimestampFloorExprMacro;
import io.druid.query.expression.TimestampFormatExprMacro;
import io.druid.query.expression.TimestampParseExprMacro;
import io.druid.query.expression.TimestampShiftExprMacro;
import io.druid.query.expression.TrimExprMacro;
import io.druid.query.scan.ScanQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.StringUtils;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandlerUtils.class);

  private static final int NUM_RETRIES = 8;
  private static final int SECONDS_BETWEEN_RETRIES = 2;
  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB
  private static final int DEFAULT_STREAMING_RESULT_SIZE = 100;
  private static final String SMILE_CONTENT_TYPE = "application/x-jackson-smile";
  //Druid storage timestamp column name
  public static final String DEFAULT_TIMESTAMP_COLUMN = "__time";
  //Druid Json timestamp column name
  public static final String EVENT_TIMESTAMP_COLUMN = "timestamp";
  public static final String INDEX_ZIP = "index.zip";
  public static final String DESCRIPTOR_JSON = "descriptor.json";
  public static final Interval DEFAULT_INTERVAL = new Interval(
          new DateTime("1900-01-01", ISOChronology.getInstanceUTC()),
          new DateTime("3000-01-01", ISOChronology.getInstanceUTC())
  ).withChronology(ISOChronology.getInstanceUTC());

  /**
   * Mapper to use to serialize/deserialize Druid objects (JSON)
   */
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  /**
   * Mapper to use to serialize/deserialize Druid objects (SMILE)
   */
  public static final ObjectMapper SMILE_MAPPER = new DefaultObjectMapper(new SmileFactory());
  private static final int DEFAULT_MAX_TRIES = 10;

  static {
    // This is needed for serde of PagingSpec as it uses JacksonInject for injecting SelectQueryConfig
    InjectableValues.Std injectableValues = new InjectableValues.Std()
        .addValue(SelectQueryConfig.class, new SelectQueryConfig(false))
        // Expressions macro table used when we deserialize the query from calcite plan
        .addValue(ExprMacroTable.class, new ExprMacroTable(ImmutableList
            .of(new LikeExprMacro(),
                new RegexpExtractExprMacro(),
                new TimestampCeilExprMacro(),
                new TimestampExtractExprMacro(),
                new TimestampFormatExprMacro(),
                new TimestampParseExprMacro(),
                new TimestampShiftExprMacro(),
                new TimestampFloorExprMacro(),
                new TrimExprMacro.BothTrimExprMacro(),
                new TrimExprMacro.LeftTrimExprMacro(),
                new TrimExprMacro.RightTrimExprMacro()
            )))
        .addValue(ObjectMapper.class, JSON_MAPPER)
        .addValue(DataSegment.PruneLoadSpecHolder.class, DataSegment.PruneLoadSpecHolder.DEFAULT);

    JSON_MAPPER.setInjectableValues(injectableValues);
    SMILE_MAPPER.setInjectableValues(injectableValues);
    // Register the shard sub type to be used by the mapper
    JSON_MAPPER.registerSubtypes(new NamedType(LinearShardSpec.class, "linear"));
    JSON_MAPPER.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
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
   * Used by druid to perform IO on indexes
   */
  public static final IndexIO INDEX_IO =
      new IndexIO(JSON_MAPPER, TmpFileSegmentWriteOutMediumFactory.instance(), () -> 0);

  /**
   * Used by druid to merge indexes
   */
  public static final IndexMergerV9 INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER,
          DruidStorageHandlerUtils.INDEX_IO,TmpFileSegmentWriteOutMediumFactory.instance()
  );

  /**
   * Generic Interner implementation used to read segments object from metadata storage
   */
  public static final Interner<DataSegment> DATA_SEGMENT_INTERNER = Interners.newWeakInterner();

  /**
   * Method that creates a request for Druid query using SMILE format.
   *
   * @param address
   * @param query
   *
   * @return
   *
   * @throws IOException
   */
  public static Request createSmileRequest(String address, io.druid.query.Query query)
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
  public static List<DataSegment> getCreatedSegments(Path taskDir, Configuration conf)
          throws IOException {
    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();
    FileSystem fs = taskDir.getFileSystem(conf);
    FileStatus[] fss;
    fss = fs.listStatus(taskDir);
    for (FileStatus fileStatus : fss) {
      final DataSegment segment = JSON_MAPPER
              .readValue((InputStream) fs.open(fileStatus.getPath()), DataSegment.class);
      publishedSegmentsBuilder.add(segment);
    }
    return publishedSegmentsBuilder.build();
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
            DataPusher.class, () -> {
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
            (HandleCallback<List<String>>) handle -> handle.createQuery(
                    String.format("SELECT DISTINCT(datasource) FROM %s WHERE used = true",
                            metadataStorageTablesConfig.getSegmentsTable()
                    ))
                    .fold(Lists.<String>newArrayList(),
                        (druidDataSources, stringObjectMap, foldController, statementContext) -> {
                          druidDataSources.add(
                                  MapUtils.getString(stringObjectMap, "datasource")
                          );
                          return druidDataSources;
                        }
                    )
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
        LOG.warn("Cannot delete data source {}, does not exist", dataSource);
        return false;
      }

      connector.getDBI().withHandle(
              (HandleCallback<Void>) handle -> {
                disableDataSourceWithHandle(handle, metadataStorageTablesConfig, dataSource);
                return null;
              }
      );

    } catch (Exception e) {
      LOG.error(String.format("Error removing dataSource %s", dataSource), e);
      return false;
    }
    return true;
  }

  /**
   * First computes the segments timeline to accommodate new segments for insert into case
   * Then moves segments to druid deep storage with updated metadata/version
   * ALL IS DONE IN ONE TRANSACTION
   *
   * @param connector DBI connector to commit
   * @param metadataStorageTablesConfig Druid metadata tables definitions
   * @param dataSource Druid datasource name
   * @param segments List of segments to move and commit to metadata
   * @param overwrite if it is an insert overwrite
   * @param conf Configuration
   * @param dataSegmentPusher segment pusher
   *
   * @return List of successfully published Druid segments.
   * This list has the updated versions and metadata about segments after move and timeline sorting
   *
   * @throws CallbackFailedException
   */
  public static List<DataSegment> publishSegmentsAndCommit(final SQLMetadataConnector connector,
          final MetadataStorageTablesConfig metadataStorageTablesConfig,
          final String dataSource,
          final List<DataSegment> segments,
          boolean overwrite,
          Configuration conf,
          DataSegmentPusher dataSegmentPusher
  ) throws CallbackFailedException {
    return connector.getDBI().inTransaction(
            (handle, transactionStatus) -> {
              // We create the timeline for the existing and new segments
              VersionedIntervalTimeline<String, DataSegment> timeline;
              if (overwrite) {
                // If we are overwriting, we disable existing sources
                disableDataSourceWithHandle(handle, metadataStorageTablesConfig, dataSource);

                // When overwriting, we just start with empty timeline,
                // as we are overwriting segments with new versions
                timeline = new VersionedIntervalTimeline<>(Ordering.natural());
              } else {
                // Append Mode
                if (segments.isEmpty()) {
                  // If there are no new segments, we can just bail out
                  return Collections.EMPTY_LIST;
                }
                // Otherwise, build a timeline of existing segments in metadata storage
                Interval indexedInterval = JodaUtils
                        .umbrellaInterval(Iterables.transform(segments,
                                input -> input.getInterval()
                        ));
                LOG.info("Building timeline for umbrella Interval [{}]", indexedInterval);
                timeline = getTimelineForIntervalWithHandle(
                        handle, dataSource, indexedInterval, metadataStorageTablesConfig);
              }

              final List<DataSegment> finalSegmentsToPublish = Lists.newArrayList();
              for (DataSegment segment : segments) {
                List<TimelineObjectHolder<String, DataSegment>> existingChunks = timeline
                        .lookup(segment.getInterval());
                if (existingChunks.size() > 1) {
                  // Not possible to expand since we have more than one chunk with a single segment.
                  // This is the case when user wants to append a segment with coarser granularity.
                  // e.g If metadata storage already has segments for with granularity HOUR and segments to append have DAY granularity.
                  // Druid shard specs does not support multiple partitions for same interval with different granularity.
                  throw new IllegalStateException(
                          String.format(
                                  "Cannot allocate new segment for dataSource[%s], interval[%s], already have [%,d] chunks. Not possible to append new segment.",
                                  dataSource,
                                  segment.getInterval(),
                                  existingChunks.size()
                          )
                  );
                }
                // Find out the segment with latest version and maximum partition number
                SegmentIdentifier max = null;
                final ShardSpec newShardSpec;
                final String newVersion;
                if (!existingChunks.isEmpty()) {
                  // Some existing chunk, Find max
                  TimelineObjectHolder<String, DataSegment> existingHolder = Iterables
                          .getOnlyElement(existingChunks);
                  for (PartitionChunk<DataSegment> existing : existingHolder.getObject()) {
                    if (max == null ||
                            max.getShardSpec().getPartitionNum() < existing.getObject()
                                    .getShardSpec()
                                    .getPartitionNum()) {
                      max = SegmentIdentifier.fromDataSegment(existing.getObject());
                    }
                  }
                }

                if (max == null) {
                  // No existing shard present in the database, use the current version.
                  newShardSpec = segment.getShardSpec();
                  newVersion = segment.getVersion();
                } else {
                  // use version of existing max segment to generate new shard spec
                  newShardSpec = getNextPartitionShardSpec(max.getShardSpec());
                  newVersion = max.getVersion();
                }
                DataSegment publishedSegment = publishSegmentWithShardSpec(
                        segment,
                        newShardSpec,
                        newVersion,
                        getPath(segment).getFileSystem(conf),
                        dataSegmentPusher
                );
                finalSegmentsToPublish.add(publishedSegment);
                timeline.add(
                        publishedSegment.getInterval(),
                        publishedSegment.getVersion(),
                        publishedSegment.getShardSpec().createChunk(publishedSegment)
                );

              }

              // Publish new segments to metadata storage
              final PreparedBatch batch = handle.prepareBatch(
                      String.format(
                              "INSERT INTO %1$s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
                                      + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                              metadataStorageTablesConfig.getSegmentsTable()
                      )

              );

              for (final DataSegment segment : finalSegmentsToPublish) {

                batch.add(
                        new ImmutableMap.Builder<String, Object>()
                                .put("id", segment.getIdentifier())
                                .put("dataSource", segment.getDataSource())
                                .put("created_date", new DateTime().toString())
                                .put("start", segment.getInterval().getStart().toString())
                                .put("end", segment.getInterval().getEnd().toString())
                                .put("partitioned",
                                        (segment.getShardSpec() instanceof NoneShardSpec) ?
                                                false :
                                                true
                                )
                                .put("version", segment.getVersion())
                                .put("used", true)
                                .put("payload", JSON_MAPPER.writeValueAsBytes(segment))
                                .build()
                );

                LOG.info("Published {}", segment.getIdentifier());
              }
              batch.execute();

              return finalSegmentsToPublish;
            }
    );
  }

  public static void disableDataSourceWithHandle(Handle handle,
          MetadataStorageTablesConfig metadataStorageTablesConfig, String dataSource
  ) {
    handle.createStatement(
            String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource",
                    metadataStorageTablesConfig.getSegmentsTable()
            )
    )
            .bind("dataSource", dataSource)
            .execute();
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
            (handle, status) -> handle
                    .createQuery(String.format(
                            "SELECT payload FROM %s WHERE dataSource = :dataSource",
                            metadataStorageTablesConfig.getSegmentsTable()
                    ))
                    .setFetchSize(getStreamingFetchSize(connector))
                    .bind("dataSource", dataSource)
                    .map(ByteArrayMapper.FIRST)
                    .fold(
                            new ArrayList<>(),
                            (Folder3<List<DataSegment>, byte[]>) (accumulator, payload, control, ctx) -> {
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
                    )
            , 3, DEFAULT_MAX_TRIES);
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

  public static String createScanAllQuery(String dataSourceName) throws JsonProcessingException {
    final ScanQuery.ScanQueryBuilder scanQueryBuilder = ScanQuery.newScanQueryBuilder();
    final List<Interval> intervals = Arrays.asList(DEFAULT_INTERVAL);
    ScanQuery scanQuery = scanQueryBuilder
        .dataSource(dataSourceName)
        .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
        .intervals(new MultipleIntervalSegmentSpec(intervals))
        .build();
    return JSON_MAPPER.writeValueAsString(scanQuery);
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

  private static VersionedIntervalTimeline<String, DataSegment> getTimelineForIntervalWithHandle(
          final Handle handle,
          final String dataSource,
          final Interval interval,
          final MetadataStorageTablesConfig dbTables
  ) throws IOException {
    Query<Map<String, Object>> sql = handle.createQuery(
            String.format(
                    "SELECT payload FROM %s WHERE used = true AND dataSource = ? AND start <= ? AND \"end\" >= ?",
                    dbTables.getSegmentsTable()
            )
    ).bind(0, dataSource)
            .bind(1, interval.getEnd().toString())
            .bind(2, interval.getStart().toString());

    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(
            Ordering.natural()
    );
    final ResultIterator<byte[]> dbSegments = sql
            .map(ByteArrayMapper.FIRST)
            .iterator();
    try {
      while (dbSegments.hasNext()) {
        final byte[] payload = dbSegments.next();
        DataSegment segment = JSON_MAPPER.readValue(
                payload,
                DataSegment.class
        );
        timeline.add(segment.getInterval(), segment.getVersion(),
                segment.getShardSpec().createChunk(segment)
        );
      }
    } finally {
      dbSegments.close();
    }
    return timeline;
  }

  public static DataSegmentPusher createSegmentPusherForDirectory(String segmentDirectory,
          Configuration configuration) throws IOException {
    final HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    return new HdfsDataSegmentPusher(
            hdfsDataSegmentPusherConfig, configuration, JSON_MAPPER);
  }

  public static DataSegment publishSegmentWithShardSpec(DataSegment segment, ShardSpec shardSpec,
          String version, FileSystem fs, DataSegmentPusher dataSegmentPusher
  ) throws IOException {
    boolean retry = true;
    DataSegment.Builder dataSegmentBuilder = new DataSegment.Builder(segment).version(version);
    Path finalPath = null;
    while (retry) {
      retry = false;
      dataSegmentBuilder.shardSpec(shardSpec);
      final Path intermediatePath = getPath(segment);

      finalPath = new Path(dataSegmentPusher.getPathForHadoop(), dataSegmentPusher
              .makeIndexPathName(dataSegmentBuilder.build(), DruidStorageHandlerUtils.INDEX_ZIP));
      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(finalPath.getParent());

      if (!fs.rename(intermediatePath, finalPath)) {
        if (fs.exists(finalPath)) {
          // Someone else is also trying to append
          shardSpec = getNextPartitionShardSpec(shardSpec);
          retry = true;
        } else {
          throw new IOException(String.format(
                  "Failed to rename intermediate segment[%s] to final segment[%s] is not present.",
                  intermediatePath,
                  finalPath
          ));
        }
      }
    }
    DataSegment dataSegment = dataSegmentBuilder
            .loadSpec(dataSegmentPusher.makeLoadSpec(finalPath.toUri()))
            .build();

    writeSegmentDescriptor(fs, dataSegment, new Path(finalPath.getParent(), DruidStorageHandlerUtils.DESCRIPTOR_JSON));

    return dataSegment;
  }

  private static ShardSpec getNextPartitionShardSpec(ShardSpec shardSpec) {
    if (shardSpec instanceof LinearShardSpec) {
      return new LinearShardSpec(shardSpec.getPartitionNum() + 1);
    } else if (shardSpec instanceof NumberedShardSpec) {
      return new NumberedShardSpec(shardSpec.getPartitionNum(),
              ((NumberedShardSpec) shardSpec).getPartitions()
      );
    } else {
      // Druid only support appending more partitions to Linear and Numbered ShardSpecs.
      throw new IllegalStateException(
              String.format(
                      "Cannot expand shard spec [%s]",
                      shardSpec
              )
      );
    }
  }

  public static Path getPath(DataSegment dataSegment) {
    return new Path(String.valueOf(dataSegment.getLoadSpec().get("path")));
  }

  public static GranularitySpec getGranularitySpec(Configuration configuration, Properties tableProperties) {
    final String segmentGranularity =
        tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) != null ?
            tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) :
            HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY);
    return new UniformGranularitySpec(
        Granularity.fromString(segmentGranularity),
        Granularity.fromString(
            tableProperties.getProperty(Constants.DRUID_QUERY_GRANULARITY) == null
                ? "NONE"
                : tableProperties.getProperty(Constants.DRUID_QUERY_GRANULARITY)),
        null
    );
  }

  public static IndexSpec getIndexSpec(Configuration jc) {
    IndexSpec indexSpec;
    if ("concise".equals(HiveConf.getVar(jc, HiveConf.ConfVars.HIVE_DRUID_BITMAP_FACTORY_TYPE))) {
      indexSpec = new IndexSpec(new ConciseBitmapSerdeFactory(), null, null, null);
    } else {
      indexSpec = new IndexSpec(new RoaringBitmapSerdeFactory(true), null, null, null);
    }
    return indexSpec;
  }

  public static Pair<List<DimensionSchema>, AggregatorFactory[]> getDimensionsAndAggregates(Configuration jc, List<String> columnNames,
      List<TypeInfo> columnTypes) {
    // Default, all columns that are not metrics or timestamp, are treated as dimensions
    final List<DimensionSchema> dimensions = new ArrayList<>();
    ImmutableList.Builder<AggregatorFactory> aggregatorFactoryBuilder = ImmutableList.builder();
    for (int i = 0; i < columnTypes.size(); i++) {
      final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) columnTypes
          .get(i)).getPrimitiveCategory();
      AggregatorFactory af;
      switch (primitiveCategory) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        af = new LongSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
        break;
      case FLOAT:
        af = new FloatSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
        break;
      case DOUBLE:
        af = new DoubleSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
        break;
      case DECIMAL:
        throw new UnsupportedOperationException(
            String.format("Druid does not support decimal column type cast column "
                + "[%s] to double", columnNames.get(i)));

      case TIMESTAMP:
        // Granularity column
        String tColumnName = columnNames.get(i);
        if (!tColumnName.equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME) &&
            !tColumnName.equals(DruidStorageHandlerUtils.DEFAULT_TIMESTAMP_COLUMN)) {
          throw new IllegalArgumentException(
              "Dimension " + tColumnName + " does not have STRING type: " +
                  primitiveCategory);
        }
        continue;
      case TIMESTAMPLOCALTZ:
        // Druid timestamp column
        String tLocalTZColumnName = columnNames.get(i);
        if (!tLocalTZColumnName.equals(DruidStorageHandlerUtils.DEFAULT_TIMESTAMP_COLUMN)) {
          throw new IllegalArgumentException(
              "Dimension " + tLocalTZColumnName + " does not have STRING type: " +
                  primitiveCategory);
        }
        continue;
      default:
        // Dimension
        String dColumnName = columnNames.get(i);
        if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(primitiveCategory) !=
            PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
            && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
          throw new IllegalArgumentException(
              "Dimension " + dColumnName + " does not have STRING type: " +
                  primitiveCategory);
        }
        dimensions.add(new StringDimensionSchema(dColumnName));
        continue;
      }
      aggregatorFactoryBuilder.add(af);
    }
    ImmutableList<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();
    return Pair.of(dimensions,
        aggregatorFactories.toArray(new AggregatorFactory[aggregatorFactories.size()]));
  }
}
