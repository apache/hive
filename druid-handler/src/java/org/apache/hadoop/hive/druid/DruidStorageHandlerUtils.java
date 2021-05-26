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
package org.apache.hadoop.hive.druid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.LikeExprMacro;
import org.apache.druid.query.expression.RegexpExtractExprMacro;
import org.apache.druid.query.expression.TimestampCeilExprMacro;
import org.apache.druid.query.expression.TimestampExtractExprMacro;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampFormatExprMacro;
import org.apache.druid.query.expression.TimestampParseExprMacro;
import org.apache.druid.query.expression.TimestampShiftExprMacro;
import org.apache.druid.query.expression.TrimExprMacro;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.filter.BloomKFilterHolder;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusher;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.json.AvroParseSpec;
import org.apache.hadoop.hive.druid.json.AvroStreamInputRowParser;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.ExprNodeDynamicValueEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToString;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {
  private DruidStorageHandlerUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandlerUtils.class);

  private static final int NUM_RETRIES = 8;
  private static final int SECONDS_BETWEEN_RETRIES = 2;
  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB
  private static final int DEFAULT_STREAMING_RESULT_SIZE = 100;
  private static final String SMILE_CONTENT_TYPE = "application/x-jackson-smile";

  static final String INDEX_ZIP = "index.zip";
  private static final String DESCRIPTOR_JSON = "descriptor.json";
  private static final Interval
      DEFAULT_INTERVAL =
      new Interval(new DateTime("1900-01-01", ISOChronology.getInstanceUTC()),
          new DateTime("3000-01-01", ISOChronology.getInstanceUTC())).withChronology(ISOChronology.getInstanceUTC());

  /**
   * Mapper to use to serialize/deserialize Druid objects (JSON).
   */
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  /**
   * Mapper to use to serialize/deserialize Druid objects (SMILE).
   */
  public static final ObjectMapper SMILE_MAPPER = new DefaultObjectMapper(new SmileFactory());
  private static final int DEFAULT_MAX_TRIES = 10;

  static {
    // This is needed to initliaze NullHandling for druid without guice.
    NullHandling.initializeForTests();
    // This is needed for serde of PagingSpec as it uses JacksonInject for injecting SelectQueryConfig
    InjectableValues.Std injectableValues = new InjectableValues.Std()
        // Expressions macro table used when we deserialize the query from calcite plan
        .addValue(ExprMacroTable.class, new ExprMacroTable(ImmutableList
            .of(new LikeExprMacro(), new RegexpExtractExprMacro(), new TimestampCeilExprMacro(),
                new TimestampExtractExprMacro(), new TimestampFormatExprMacro(), new TimestampParseExprMacro(),
                new TimestampShiftExprMacro(), new TimestampFloorExprMacro(), new TrimExprMacro.BothTrimExprMacro(),
                new TrimExprMacro.LeftTrimExprMacro(), new TrimExprMacro.RightTrimExprMacro())))
        .addValue(ObjectMapper.class, JSON_MAPPER)
        .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);

    JSON_MAPPER.setInjectableValues(injectableValues);
    SMILE_MAPPER.setInjectableValues(injectableValues);
    // Register the shard sub type to be used by the mapper
    JSON_MAPPER.registerSubtypes(new NamedType(LinearShardSpec.class, "linear"));
    JSON_MAPPER.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
    JSON_MAPPER.registerSubtypes(new NamedType(AvroParseSpec.class, "avro"));
    SMILE_MAPPER.registerSubtypes(new NamedType(AvroParseSpec.class, "avro"));
    JSON_MAPPER.registerSubtypes(new NamedType(AvroStreamInputRowParser.class, "avro_stream"));
    SMILE_MAPPER.registerSubtypes(new NamedType(AvroStreamInputRowParser.class, "avro_stream"));
    // Register Bloom Filter Serializers
    BloomFilterSerializersModule bloomFilterSerializersModule = new BloomFilterSerializersModule();
    JSON_MAPPER.registerModule(bloomFilterSerializersModule);
    SMILE_MAPPER.registerModule(bloomFilterSerializersModule);

    // set the timezone of the object mapper
    // THIS IS NOT WORKING workaround is to set it as part of java opts -Duser.timezone="UTC"
    JSON_MAPPER.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      // No operation emitter will be used by some internal druid classes.
      EmittingLogger.registerEmitter(new ServiceEmitter("druid-hive-indexer",
          InetAddress.getLocalHost().getHostName(),
          new NoopEmitter()));
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Used by druid to perform IO on indexes.
   */
  public static final IndexIO INDEX_IO = new IndexIO(JSON_MAPPER, new DruidProcessingConfig() {
    @Override public String getFormatString() {
      return "%s-%s";
    }
  });

  /**
   * Used by druid to merge indexes.
   */
  public static final IndexMergerV9
      INDEX_MERGER_V9 =
      new IndexMergerV9(JSON_MAPPER, DruidStorageHandlerUtils.INDEX_IO, TmpFileSegmentWriteOutMediumFactory.instance());

  /**
   * Generic Interner implementation used to read segments object from metadata storage.
   */
  private static final Interner<DataSegment> DATA_SEGMENT_INTERNER = Interners.newWeakInterner();

  /**
   * Method that creates a request for Druid query using SMILE format.
   *
   * @param address of the host target.
   * @param query   druid query.
   * @return Request object to be submitted.
   */
  public static Request createSmileRequest(String address, org.apache.druid.query.Query query) {
    try {
      return new Request(HttpMethod.POST, new URL(String.format("%s/druid/v2/", "http://" + address))).setContent(
          SMILE_MAPPER.writeValueAsBytes(query)).setHeader(HttpHeaders.Names.CONTENT_TYPE, SMILE_CONTENT_TYPE);
    } catch (MalformedURLException e) {
      LOG.error("URL Malformed  address {}", address);
      throw new RuntimeException(e);
    } catch (JsonProcessingException e) {
      LOG.error("can not Serialize the Query [{}]", query.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * Method that submits a request to an Http address and retrieves the result.
   * The caller is responsible for closing the stream once it finishes consuming it.
   *
   * @param client  Http Client will be used to submit request.
   * @param request Http request to be submitted.
   * @return response object.
   * @throws IOException in case of request IO error.
   */
  public static InputStream submitRequest(HttpClient client, Request request) throws IOException {
    try {
      return client.go(request, new InputStreamResponseHandler()).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e.getCause());
    }

  }

  static StringFullResponseHolder getResponseFromCurrentLeader(HttpClient client, Request request,
      StringFullResponseHandler fullResponseHandler) throws ExecutionException, InterruptedException {
    StringFullResponseHolder responseHolder = client.go(request, fullResponseHandler).get();
    if (HttpResponseStatus.TEMPORARY_REDIRECT.equals(responseHolder.getStatus())) {
      String redirectUrlStr = responseHolder.getResponse().headers().get("Location");
      LOG.debug("Request[%s] received redirect response to location [%s].", request.getUrl(), redirectUrlStr);
      final URL redirectUrl;
      try {
        redirectUrl = new URL(redirectUrlStr);
      } catch (MalformedURLException ex) {
        throw new ExecutionException(String
            .format("Malformed redirect location is found in response from url[%s], new location[%s].",
                request.getUrl(),
            redirectUrlStr), ex);
      }
      responseHolder = client.go(withUrl(request, redirectUrl), fullResponseHandler).get();
    }
    return responseHolder;
  }

  private static Request withUrl(Request old, URL url) {
    Request req = new Request(old.getMethod(), url);
    req.addHeaderValues(old.getHeaders());
    if (old.hasContent()) {
      req.setContent(old.getContent());
    }
    return req;
  }

  /**
   * @param taskDir path to the  directory containing the segments descriptor info
   *                the descriptor path will be
   *                ../workingPath/task_id/{@link DruidStorageHandler#SEGMENTS_DESCRIPTOR_DIR_NAME}/*.json
   * @param conf    hadoop conf to get the file system
   * @return List of DataSegments
   * @throws IOException can be for the case we did not produce data.
   */
  public static List<DataSegment> getCreatedSegments(Path taskDir, Configuration conf) throws IOException {
    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();
    FileSystem fs = taskDir.getFileSystem(conf);
    FileStatus[] fss;
    fss = fs.listStatus(taskDir);
    for (FileStatus fileStatus : fss) {
      final DataSegment segment = JSON_MAPPER.readValue((InputStream) fs.open(fileStatus.getPath()), DataSegment.class);
      publishedSegmentsBuilder.add(segment);
    }
    return publishedSegmentsBuilder.build();
  }

  /**
   * Writes to filesystem serialized form of segment descriptor if an existing file exists it will try to replace it.
   *
   * @param outputFS       filesystem.
   * @param segment        DataSegment object.
   * @param descriptorPath path.
   * @throws IOException in case any IO issues occur.
   */
  public static void writeSegmentDescriptor(final FileSystem outputFS,
      final DataSegment segment,
      final Path descriptorPath) throws IOException {
    final DataPusher descriptorPusher = (DataPusher) RetryProxy.create(DataPusher.class, () -> {
      if (outputFS.exists(descriptorPath)) {
        if (!outputFS.delete(descriptorPath, false)) {
          throw new IOException(String.format("Failed to delete descriptor at [%s]", descriptorPath));
        }
      }
      try (final OutputStream descriptorOut = outputFS.create(descriptorPath, true, DEFAULT_FS_BUFFER_SIZE)) {
        JSON_MAPPER.writeValue(descriptorOut, segment);
        descriptorOut.flush();
      }
    }, RetryPolicies.exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS));
    descriptorPusher.push();
  }

  /**
   * @param connector                   SQL metadata connector to the metadata storage
   * @param metadataStorageTablesConfig Table config
   * @return all the active data sources in the metadata storage
   */
  static Collection<String> getAllDataSourceNames(SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig) {
    return connector.getDBI()
        .withHandle((HandleCallback<List<String>>) handle -> handle.createQuery(String.format(
            "SELECT DISTINCT(datasource) FROM %s WHERE used = true",
            metadataStorageTablesConfig.getSegmentsTable()))
            .fold(Lists.<String>newArrayList(),
                (druidDataSources, stringObjectMap, foldController, statementContext) -> {
                  druidDataSources.add(MapUtils.getString(stringObjectMap, "datasource"));
                  return druidDataSources;
                }));
  }

  /**
   * @param connector                   SQL connector to metadata
   * @param metadataStorageTablesConfig Tables configuration
   * @param dataSource                  Name of data source
   * @return true if the data source was successfully disabled false otherwise
   */
  static boolean disableDataSource(SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig,
      final String dataSource) {
    try {
      if (!getAllDataSourceNames(connector, metadataStorageTablesConfig).contains(dataSource)) {
        LOG.warn("Cannot delete data source {}, does not exist", dataSource);
        return false;
      }

      connector.getDBI().withHandle((HandleCallback<Void>) handle -> {
        disableDataSourceWithHandle(handle, metadataStorageTablesConfig, dataSource);
        return null;
      });

    } catch (Exception e) {
      LOG.error(String.format("Error removing dataSource %s", dataSource), e);
      return false;
    }
    return true;
  }

  /**
   * First computes the segments timeline to accommodate new segments for insert into case.
   * Then moves segments to druid deep storage with updated metadata/version.
   * ALL IS DONE IN ONE TRANSACTION
   *
   * @param connector                   DBI connector to commit
   * @param metadataStorageTablesConfig Druid metadata tables definitions
   * @param dataSource                  Druid datasource name
   * @param segments                    List of segments to move and commit to metadata
   * @param overwrite                   if it is an insert overwrite
   * @param conf                        Configuration
   * @param dataSegmentPusher           segment pusher
   * @return List of successfully published Druid segments.
   * This list has the updated versions and metadata about segments after move and timeline sorting
   * @throws CallbackFailedException in case the connector can not add the segment to the DB.
   */
  @SuppressWarnings("unchecked") static List<DataSegment> publishSegmentsAndCommit(final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig,
      final String dataSource,
      final List<DataSegment> segments,
      boolean overwrite,
      Configuration conf,
      DataSegmentPusher dataSegmentPusher) throws CallbackFailedException {
    return connector.getDBI().inTransaction((handle, transactionStatus) -> {
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
        Interval
            indexedInterval =
            JodaUtils.umbrellaInterval(segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()));
        LOG.info("Building timeline for umbrella Interval [{}]", indexedInterval);
        timeline = getTimelineForIntervalWithHandle(handle, dataSource, indexedInterval, metadataStorageTablesConfig);
      }

      final List<DataSegment> finalSegmentsToPublish = Lists.newArrayList();
      for (DataSegment segment : segments) {
        List<TimelineObjectHolder<String, DataSegment>> existingChunks = timeline.lookup(segment.getInterval());
        if (existingChunks.size() > 1) {
          // Not possible to expand since we have more than one chunk with a single segment.
          // This is the case when user wants to append a segment with coarser granularity.
          // case metadata storage has segments with granularity HOUR and segments to append have DAY granularity.
          // Druid shard specs does not support multiple partitions for same interval with different granularity.
          throw new IllegalStateException(String.format(
              "Cannot allocate new segment for dataSource[%s], interval[%s], already have [%,d] chunks. "
                  + "Not possible to append new segment.",
              dataSource,
              segment.getInterval(),
              existingChunks.size()));
        }
        // Find out the segment with latest version and maximum partition number
        SegmentIdWithShardSpec max = null;
        final ShardSpec newShardSpec;
        final String newVersion;
        if (!existingChunks.isEmpty()) {
          // Some existing chunk, Find max
          TimelineObjectHolder<String, DataSegment> existingHolder = Iterables.getOnlyElement(existingChunks);
          for (PartitionChunk<DataSegment> existing : existingHolder.getObject()) {
            if (max == null || max.getShardSpec().getPartitionNum() < existing.getObject()
                .getShardSpec()
                .getPartitionNum()) {
              max = SegmentIdWithShardSpec.fromDataSegment(existing.getObject());
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
        DataSegment
            publishedSegment =
            publishSegmentWithShardSpec(segment,
                newShardSpec,
                newVersion,
                getPath(segment).getFileSystem(conf),
                dataSegmentPusher);
        finalSegmentsToPublish.add(publishedSegment);
        timeline.add(publishedSegment.getInterval(),
            publishedSegment.getVersion(),
            publishedSegment.getShardSpec().createChunk(publishedSegment));

      }

      // Publish new segments to metadata storage
      final PreparedBatch
          batch =
          handle.prepareBatch(String.format(
              "INSERT INTO %1$s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
                  + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
              metadataStorageTablesConfig.getSegmentsTable())

          );

      for (final DataSegment segment : finalSegmentsToPublish) {

        batch.add(new ImmutableMap.Builder<String, Object>().put("id", segment.getId().toString())
            .put("dataSource", segment.getDataSource())
            .put("created_date", new DateTime().toString())
            .put("start", segment.getInterval().getStart().toString())
            .put("end", segment.getInterval().getEnd().toString())
            .put("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
            .put("version", segment.getVersion())
            .put("used", true)
            .put("payload", JSON_MAPPER.writeValueAsBytes(segment))
            .build());

        LOG.info("Published {}", segment.getId().toString());
      }
      batch.execute();

      return finalSegmentsToPublish;
    });
  }

  private static void disableDataSourceWithHandle(Handle handle,
      MetadataStorageTablesConfig metadataStorageTablesConfig,
      String dataSource) {
    handle.createStatement(String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource",
        metadataStorageTablesConfig.getSegmentsTable())).bind("dataSource", dataSource).execute();
  }

  /**
   * @param connector                   SQL connector to metadata
   * @param metadataStorageTablesConfig Tables configuration
   * @param dataSource                  Name of data source
   * @return List of all data segments part of the given data source
   */
  static List<DataSegment> getDataSegmentList(final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig,
      final String dataSource) {
    return connector.retryTransaction((handle, status) -> handle.createQuery(String.format(
        "SELECT payload FROM %s WHERE dataSource = :dataSource",
        metadataStorageTablesConfig.getSegmentsTable()))
        .setFetchSize(getStreamingFetchSize(connector))
        .bind("dataSource", dataSource)
        .map(ByteArrayMapper.FIRST)
        .fold(new ArrayList<>(), (Folder3<List<DataSegment>, byte[]>) (accumulator, payload, control, ctx) -> {
          try {
            final DataSegment segment = DATA_SEGMENT_INTERNER.intern(JSON_MAPPER.readValue(payload, DataSegment.class));

            accumulator.add(segment);
            return accumulator;
          } catch (Exception e) {
            throw new SQLException(e.toString());
          }
        }), 3, DEFAULT_MAX_TRIES);
  }

  /**
   * @param connector SQL DBI connector.
   * @return streaming fetch size.
   */
  private static int getStreamingFetchSize(SQLMetadataConnector connector) {
    if (connector instanceof MySQLConnector) {
      return Integer.MIN_VALUE;
    }
    return DEFAULT_STREAMING_RESULT_SIZE;
  }

  /**
   * @param pushedSegment         the pushed data segment object
   * @param segmentsDescriptorDir actual directory path for descriptors.
   * @return a sanitize file name
   */
  public static Path makeSegmentDescriptorOutputPath(DataSegment pushedSegment, Path segmentsDescriptorDir) {
    return new Path(segmentsDescriptorDir, String.format("%s.json", pushedSegment.getId().toString().replace(":", "")));
  }

  public static String createScanAllQuery(String dataSourceName, List<String> columns) throws JsonProcessingException {
    final Druids.ScanQueryBuilder scanQueryBuilder = Druids.newScanQueryBuilder();
    final List<Interval> intervals = Collections.singletonList(DEFAULT_INTERVAL);
    ScanQuery
        scanQuery =
        scanQueryBuilder.dataSource(dataSourceName).resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
            .intervals(new MultipleIntervalSegmentSpec(intervals))
            .columns(columns)
            .build();
    return JSON_MAPPER.writeValueAsString(scanQuery);
  }

  @Nullable static Boolean getBooleanProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    return Boolean.parseBoolean(val);
  }

  static boolean getBooleanProperty(Table table, String propertyName, boolean defaultVal) {
    Boolean val = getBooleanProperty(table, propertyName);
    return val == null ? defaultVal : val;
  }

  @Nullable static Integer getIntegerProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(String.format("Exception while parsing property[%s] with Value [%s] as Integer",
          propertyName,
          val));
    }
  }

  static int getIntegerProperty(Table table, String propertyName, int defaultVal) {
    Integer val = getIntegerProperty(table, propertyName);
    return val == null ? defaultVal : val;
  }

  @Nullable static Long getLongProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(String.format("Exception while parsing property[%s] with Value [%s] as Long",
          propertyName,
          val));
    }
  }

  @Nullable static Period getPeriodProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    try {
      return Period.parse(val);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Exception while parsing property[%s] with Value [%s] as Period",
          propertyName,
          val));
    }
  }

  @Nullable public static List<String> getListProperty(Table table, String propertyName) {
    List<String> rv = new ArrayList<>();
    String values = getTableProperty(table, propertyName);
    if (values == null) {
      return null;
    }
    String[] vals = values.trim().split(",");
    for (String val : vals) {
      if (org.apache.commons.lang3.StringUtils.isNotBlank(val)) {
        rv.add(val);
      }
    }
    return rv;
  }

  static String getTableProperty(Table table, String propertyName) {
    return table.getParameters().get(propertyName);
  }

  /**
   * Simple interface for retry operations.
   */
  public interface DataPusher {
    void push() throws IOException;
  }

  private static VersionedIntervalTimeline<String, DataSegment> getTimelineForIntervalWithHandle(final Handle handle,
      final String dataSource,
      final Interval interval,
      final MetadataStorageTablesConfig dbTables) throws IOException {
    Query<Map<String, Object>>
        sql =
        handle.createQuery(String.format(
            "SELECT payload FROM %s WHERE used = true AND dataSource = ? AND start <= ? AND \"end\" >= ?",
            dbTables.getSegmentsTable()))
            .bind(0, dataSource)
            .bind(1, interval.getEnd().toString())
            .bind(2, interval.getStart().toString());

    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    try (ResultIterator<byte[]> dbSegments = sql.map(ByteArrayMapper.FIRST).iterator()) {
      while (dbSegments.hasNext()) {
        final byte[] payload = dbSegments.next();
        DataSegment segment = JSON_MAPPER.readValue(payload, DataSegment.class);
        timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
      }
    }
    return timeline;
  }

  public static DataSegmentPusher createSegmentPusherForDirectory(String segmentDirectory, Configuration configuration)
      throws IOException {
    final HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    return new HdfsDataSegmentPusher(hdfsDataSegmentPusherConfig, configuration, JSON_MAPPER);
  }

  private static DataSegment publishSegmentWithShardSpec(DataSegment segment,
      ShardSpec shardSpec,
      String version,
      FileSystem fs,
      DataSegmentPusher dataSegmentPusher) throws IOException {
    boolean retry = true;
    DataSegment.Builder dataSegmentBuilder = new DataSegment.Builder(segment).version(version);
    Path finalPath = null;
    while (retry) {
      retry = false;
      dataSegmentBuilder.shardSpec(shardSpec);
      final Path intermediatePath = getPath(segment);

      finalPath =
          new Path(dataSegmentPusher.getPathForHadoop(),
              dataSegmentPusher.makeIndexPathName(dataSegmentBuilder.build(), DruidStorageHandlerUtils.INDEX_ZIP));
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
              finalPath));
        }
      }
    }
    DataSegment dataSegment = dataSegmentBuilder.loadSpec(dataSegmentPusher.makeLoadSpec(finalPath.toUri())).build();

    writeSegmentDescriptor(fs, dataSegment, new Path(finalPath.getParent(), DruidStorageHandlerUtils.DESCRIPTOR_JSON));

    return dataSegment;
  }

  private static ShardSpec getNextPartitionShardSpec(ShardSpec shardSpec) {
    if (shardSpec instanceof LinearShardSpec) {
      return new LinearShardSpec(shardSpec.getPartitionNum() + 1);
    } else if (shardSpec instanceof NumberedShardSpec) {
      return new NumberedShardSpec(shardSpec.getPartitionNum(), ((NumberedShardSpec) shardSpec).getPartitions());
    } else {
      // Druid only support appending more partitions to Linear and Numbered ShardSpecs.
      throw new IllegalStateException(String.format("Cannot expand shard spec [%s]", shardSpec));
    }
  }

  static Path getPath(DataSegment dataSegment) {
    return new Path(String.valueOf(Objects.requireNonNull(dataSegment.getLoadSpec()).get("path")));
  }

  public static GranularitySpec getGranularitySpec(Configuration configuration, Properties tableProperties) {
    final String
        segmentGranularity =
        tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) != null ?
            tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) :
            HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY);
    final boolean
        rollup =
        tableProperties.getProperty(DruidConstants.DRUID_ROLLUP) != null ?
            Boolean.parseBoolean(tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY)) :
            HiveConf.getBoolVar(configuration, HiveConf.ConfVars.HIVE_DRUID_ROLLUP);
    return new UniformGranularitySpec(Granularity.fromString(segmentGranularity),
        Granularity.fromString(tableProperties.getProperty(DruidConstants.DRUID_QUERY_GRANULARITY) == null ?
            "NONE" :
            tableProperties.getProperty(DruidConstants.DRUID_QUERY_GRANULARITY)),
        rollup,
        null);
  }

  public static IndexSpec getIndexSpec(Configuration jc) {
    final BitmapSerdeFactory bitmapSerdeFactory;
    if ("concise".equals(HiveConf.getVar(jc, HiveConf.ConfVars.HIVE_DRUID_BITMAP_FACTORY_TYPE))) {
      bitmapSerdeFactory = new ConciseBitmapSerdeFactory();
    } else {
      bitmapSerdeFactory = new RoaringBitmapSerdeFactory(true);
    }
    return new IndexSpec(bitmapSerdeFactory,
        IndexSpec.DEFAULT_DIMENSION_COMPRESSION,
        IndexSpec.DEFAULT_METRIC_COMPRESSION,
        IndexSpec.DEFAULT_LONG_ENCODING);
  }

  public static Pair<List<DimensionSchema>, AggregatorFactory[]> getDimensionsAndAggregates(List<String> columnNames,
      List<TypeInfo> columnTypes) {
    // Default, all columns that are not metrics or timestamp, are treated as dimensions
    final List<DimensionSchema> dimensions = new ArrayList<>();
    ImmutableList.Builder<AggregatorFactory> aggregatorFactoryBuilder = ImmutableList.builder();
    for (int i = 0; i < columnTypes.size(); i++) {
      final PrimitiveObjectInspector.PrimitiveCategory
          primitiveCategory =
          ((PrimitiveTypeInfo) columnTypes.get(i)).getPrimitiveCategory();
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
        throw new UnsupportedOperationException(String.format("Druid does not support decimal column type cast column "
            + "[%s] to double", columnNames.get(i)));
      case TIMESTAMP:
        // Granularity column
        String tColumnName = columnNames.get(i);
        if (!tColumnName.equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME)
            && !tColumnName.equals(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
          throw new IllegalArgumentException("Dimension "
              + tColumnName
              + " does not have STRING type: "
              + primitiveCategory);
        }
        continue;
      case TIMESTAMPLOCALTZ:
        // Druid timestamp column
        String tLocalTZColumnName = columnNames.get(i);
        if (!tLocalTZColumnName.equals(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
          throw new IllegalArgumentException("Dimension "
              + tLocalTZColumnName
              + " does not have STRING type: "
              + primitiveCategory);
        }
        continue;
      default:
        // Dimension
        String dColumnName = columnNames.get(i);
        if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(primitiveCategory)
            != PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
            && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
          throw new IllegalArgumentException("Dimension "
              + dColumnName
              + " does not have STRING type: "
              + primitiveCategory);
        }
        dimensions.add(new StringDimensionSchema(dColumnName));
        continue;
      }
      aggregatorFactoryBuilder.add(af);
    }
    ImmutableList<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();
    return Pair.of(dimensions, aggregatorFactories.toArray(new AggregatorFactory[0]));
  }

  // Druid only supports String,Long,Float,Double selectors
  private static Set<TypeInfo> druidSupportedTypeInfos =
      ImmutableSet.<TypeInfo>of(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.charTypeInfo,
          TypeInfoFactory.varcharTypeInfo, TypeInfoFactory.byteTypeInfo, TypeInfoFactory.intTypeInfo,
          TypeInfoFactory.longTypeInfo, TypeInfoFactory.shortTypeInfo, TypeInfoFactory.doubleTypeInfo);

  private static Set<TypeInfo> stringTypeInfos =
      ImmutableSet.<TypeInfo>of(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.charTypeInfo,
          TypeInfoFactory.varcharTypeInfo);

  public static org.apache.druid.query.Query addDynamicFilters(org.apache.druid.query.Query query,
      ExprNodeGenericFuncDesc filterExpr, Configuration conf, boolean resolveDynamicValues) {
    List<VirtualColumn> virtualColumns = Arrays.asList(getVirtualColumns(query).getVirtualColumns());
    org.apache.druid.query.Query rv = query;
    DimFilter joinReductionFilter = toDruidFilter(filterExpr, conf, virtualColumns, resolveDynamicValues);
    if (joinReductionFilter != null) {
      String type = query.getType();
      DimFilter filter = new AndDimFilter(joinReductionFilter, query.getFilter());
      switch (type) {
      case org.apache.druid.query.Query.TIMESERIES:
        rv = Druids.TimeseriesQueryBuilder.copy((TimeseriesQuery) query).filters(filter)
            .virtualColumns(VirtualColumns.create(virtualColumns)).build();
        break;
      case org.apache.druid.query.Query.TOPN:
        rv = new TopNQueryBuilder((TopNQuery) query).filters(filter)
            .virtualColumns(VirtualColumns.create(virtualColumns)).build();
        break;
      case org.apache.druid.query.Query.GROUP_BY:
        rv = new GroupByQuery.Builder((GroupByQuery) query).setDimFilter(filter)
            .setVirtualColumns(VirtualColumns.create(virtualColumns)).build();
        break;
      case org.apache.druid.query.Query.SCAN:
        rv = Druids.ScanQueryBuilder.copy((ScanQuery) query).filters(filter)
            .virtualColumns(VirtualColumns.create(virtualColumns)).build();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported Query type " + type);
      }
    }
    return rv;
  }

  @Nullable private static DimFilter toDruidFilter(ExprNodeDesc filterExpr, Configuration configuration,
      List<VirtualColumn> virtualColumns, boolean resolveDynamicValues) {
    if (filterExpr == null) {
      return null;
    }
    Class<? extends GenericUDF> genericUDFClass = getGenericUDFClassFromExprDesc(filterExpr);
    if (FunctionRegistry.isOpAnd(filterExpr)) {
      Iterator<ExprNodeDesc> iterator = filterExpr.getChildren().iterator();
      List<DimFilter> delegates = Lists.newArrayList();
      while (iterator.hasNext()) {
        DimFilter filter = toDruidFilter(iterator.next(), configuration, virtualColumns, resolveDynamicValues);
        if (filter != null) {
          delegates.add(filter);
        }
      }
      if (!delegates.isEmpty()) {
        return new AndDimFilter(delegates);
      }
    }
    if (FunctionRegistry.isOpOr(filterExpr)) {
      Iterator<ExprNodeDesc> iterator = filterExpr.getChildren().iterator();
      List<DimFilter> delegates = Lists.newArrayList();
      while (iterator.hasNext()) {
        DimFilter filter = toDruidFilter(iterator.next(), configuration, virtualColumns, resolveDynamicValues);
        if (filter != null) {
          delegates.add(filter);
        }
      }
      if (!delegates.isEmpty()) {
        return new OrDimFilter(delegates);
      }
    } else if (GenericUDFBetween.class == genericUDFClass) {
      List<ExprNodeDesc> child = filterExpr.getChildren();
      String col = extractColName(child.get(1), virtualColumns);
      if (col != null) {
        try {
          StringComparator comparator = stringTypeInfos
              .contains(child.get(1).getTypeInfo()) ? StringComparators.LEXICOGRAPHIC : StringComparators.NUMERIC;
          String lower = evaluate(child.get(2), configuration, resolveDynamicValues);
          String upper = evaluate(child.get(3), configuration, resolveDynamicValues);
          return new BoundDimFilter(col, lower, upper, false, false, null, null, comparator);

        } catch (HiveException e) {
          throw new RuntimeException(e);
        }
      }
    } else if (GenericUDFInBloomFilter.class == genericUDFClass) {
      List<ExprNodeDesc> child = filterExpr.getChildren();
      String col = extractColName(child.get(0), virtualColumns);
      if (col != null) {
        try {
          BloomKFilter bloomFilter = evaluateBloomFilter(child.get(1), configuration, resolveDynamicValues);
          return new BloomDimFilter(col, BloomKFilterHolder.fromBloomKFilter(bloomFilter), null);
        } catch (HiveException | IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return null;
  }

  private static String evaluate(ExprNodeDesc desc, Configuration configuration, boolean resolveDynamicValue)
      throws HiveException {
    ExprNodeEvaluator exprNodeEvaluator = ExprNodeEvaluatorFactory.get(desc, configuration);
    if (exprNodeEvaluator instanceof ExprNodeDynamicValueEvaluator && !resolveDynamicValue) {
      return desc.getExprStringForExplain();
    } else {
      return exprNodeEvaluator.evaluate(null).toString();
    }
  }

  private static BloomKFilter evaluateBloomFilter(ExprNodeDesc desc, Configuration configuration,
      boolean resolveDynamicValue) throws HiveException, IOException {
    if (!resolveDynamicValue) {
      // return a dummy bloom filter for explain
      return new BloomKFilter(1);
    } else {
      BytesWritable bw = (BytesWritable) ExprNodeEvaluatorFactory.get(desc, configuration).evaluate(null);
      return BloomKFilter.deserialize(ByteBuffer.wrap(bw.getBytes()));
    }
  }

  @Nullable public static String extractColName(ExprNodeDesc expr, List<VirtualColumn> virtualColumns) {
    if (!druidSupportedTypeInfos.contains(expr.getTypeInfo())) {
      // This column type is currently not supported in druid.(e.g boolean)
      // We cannot pass the bloom filter to druid since bloom filter tests for exact object bytes.
      return null;
    }
    if (expr instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc) expr).getColumn();
    }

    ExprNodeGenericFuncDesc funcDesc = null;
    if (expr instanceof ExprNodeGenericFuncDesc) {
      funcDesc = (ExprNodeGenericFuncDesc) expr;
    }
    if (null == funcDesc) {
      return null;
    }
    GenericUDF udf = funcDesc.getGenericUDF();
    // bail out if its not a simple cast expression.
    if (funcDesc.getChildren().size() == 1 && funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc) {
      return null;
    }
    String columnName = ((ExprNodeColumnDesc) (funcDesc.getChildren().get(0))).getColumn();
    ValueType targetType = null;
    if (udf instanceof GenericUDFBridge) {
      Class<? extends UDF> udfClass = ((GenericUDFBridge) udf).getUdfClass();
      if (udfClass.equals(UDFToDouble.class)) {
        targetType = ValueType.DOUBLE;
      } else if (udfClass.equals(UDFToFloat.class)) {
        targetType = ValueType.FLOAT;
      } else if (udfClass.equals(UDFToLong.class)) {
        targetType = ValueType.LONG;
      }
    } else if (udf instanceof GenericUDFToString) {
      targetType = ValueType.STRING;
    }

    if (targetType == null) {
      return null;
    }
    String virtualColumnExpr = DruidQuery.format("CAST(%s, '%s')", columnName, targetType.toString());
    for (VirtualColumn column : virtualColumns) {
      if (column instanceof ExpressionVirtualColumn && ((ExpressionVirtualColumn) column).getExpression()
          .equals(virtualColumnExpr)) {
        // Found an existing virtual column with same expression, no need to add another virtual column
        return column.getOutputName();
      }
    }
    Set<String> usedColumnNames = virtualColumns.stream().map(col -> col.getOutputName()).collect(Collectors.toSet());
    final String name = SqlValidatorUtil.uniquify("vc", usedColumnNames, SqlValidatorUtil.EXPR_SUGGESTER);
    ExpressionVirtualColumn expressionVirtualColumn =
        new ExpressionVirtualColumn(name, virtualColumnExpr, targetType, ExprMacroTable.nil());
    virtualColumns.add(expressionVirtualColumn);
    return name;
  }

  public static VirtualColumns getVirtualColumns(org.apache.druid.query.Query query) {
    String type = query.getType();
    switch (type) {
    case org.apache.druid.query.Query.TIMESERIES:
      return ((TimeseriesQuery) query).getVirtualColumns();
    case org.apache.druid.query.Query.TOPN:
      return ((TopNQuery) query).getVirtualColumns();
    case org.apache.druid.query.Query.GROUP_BY:
      return ((GroupByQuery) query).getVirtualColumns();
    case org.apache.druid.query.Query.SCAN:
      return ((ScanQuery) query).getVirtualColumns();
    default:
      throw new UnsupportedOperationException("Unsupported Query type " + query);
    }
  }

  @Nullable private static Class<? extends GenericUDF> getGenericUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    return genericFuncDesc.getGenericUDF().getClass();
  }
}
