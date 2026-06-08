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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorage;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLTablesConfig;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusher;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.DruidRecordWriter;
import org.apache.hadoop.hive.druid.json.KafkaSupervisorReport;
import org.apache.hadoop.hive.druid.json.KafkaSupervisorSpec;
import org.apache.hadoop.hive.druid.security.KerberosHttpClient;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.druid.DruidStorageHandlerUtils.JSON_MAPPER;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({ "rawtypes" }) public class DruidStorageHandler extends DefaultHiveMetaHook
    implements HiveStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  private static final SessionState.LogHelper CONSOLE = new SessionState.LogHelper(LOG);

  public static final String SEGMENTS_DESCRIPTOR_DIR_NAME = "segmentsDescriptorDir";

  private static final String INTERMEDIATE_SEGMENT_DIR_NAME = "intermediateSegmentDir";

  private static final HttpClient HTTP_CLIENT;

  private static final List<String> ALLOWED_ALTER_TYPES =
      ImmutableList.of("ADDPROPS", "DROPPROPS", "ADDCOLS");

  /** Druid prefix to form the URI for authentication */
  private static final String DRUID_PREFIX = "druid:";
  /** Druid config for determining the host name */
  private static final String DRUID_HOST_NAME = "druid.zk.service.host";

  static {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      lifecycle.start();
    } catch (Exception e) {
      LOG.error("Issues with lifecycle start", e);
    }
    HTTP_CLIENT = makeHttpClient(lifecycle);
    ShutdownHookManager.addShutdownHook(lifecycle::stop);
  }

  private SQLMetadataConnector connector;

  private MetadataStorageTablesConfig druidMetadataStorageTablesConfig = null;

  private String uniqueId = null;

  private String rootWorkingDir = null;

  private Configuration conf;

  public DruidStorageHandler() {
  }

  @VisibleForTesting public DruidStorageHandler(SQLMetadataConnector connector,
      MetadataStorageTablesConfig druidMetadataStorageTablesConfig) {
    this.connector = connector;
    this.druidMetadataStorageTablesConfig = druidMetadataStorageTablesConfig;
  }

  @Override public Class<? extends InputFormat> getInputFormatClass() {
    return DruidQueryBasedInputFormat.class;
  }

  @Override public Class<? extends OutputFormat> getOutputFormatClass() {
    return DruidOutputFormat.class;
  }

  @Override public Class<? extends AbstractSerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override public HiveAuthorizationProvider getAuthorizationProvider() {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> jobSecrets) {

  }

  @Override public void preCreateTable(Table table) throws MetaException {
    if (!StringUtils.isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid");
    }
    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
    String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    if (dataSourceName != null) {
      // Already Existing datasource in Druid.
      return;
    }

    // create dataSourceName based on Hive Table name
    dataSourceName = TableName.getDbTable(table.getDbName(), table.getTableName());
    try {
      // NOTE: This just created druid_segments table in Druid metastore.
      // This is needed for the case when hive is started before any of druid services
      // and druid_segments table has not been created yet.
      getConnector().createSegmentTable();
    } catch (Exception e) {
      LOG.error("Exception while trying to create druid segments table", e);
      throw new MetaException(e.getMessage());
    }
    Collection<String>
        existingDataSources =
        DruidStorageHandlerUtils.getAllDataSourceNames(getConnector(), getDruidMetadataStorageTablesConfig());
    LOG.debug("pre-create data source with name {}", dataSourceName);
    // Check for existence of for the datasource we are going to create in druid_segments table.
    if (existingDataSources.contains(dataSourceName)) {
      throw new MetaException(String.format("Data source [%s] already existing", dataSourceName));
    }
    table.getParameters().put(Constants.DRUID_DATA_SOURCE, dataSourceName);
  }

  @Override public void rollbackCreateTable(Table table) {
    cleanWorkingDir();
  }

  @Override public void commitCreateTable(Table table) throws MetaException {
    if (DruidKafkaUtils.isKafkaStreamingTable(table)) {
      updateKafkaIngestion(table);
    }
    // For CTAS queries when user has explicitly specified the datasource.
    // We will append the data to existing druid datasource.
    this.commitInsertTable(table, false);
  }

  @Override
  public URI getURIForAuth(Table table) throws URISyntaxException{
    Map<String, String> tableProperties = HiveCustomStorageHandlerUtils.getTableProperties(table);
    String host_name = conf.get(DRUID_HOST_NAME) != null ? conf.get(DRUID_HOST_NAME) :
            HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
    String table_name = tableProperties.get(Constants.DRUID_DATA_SOURCE);
    String column_names = tableProperties.get(Constants.DRUID_QUERY_FIELD_NAMES);
    if (column_names != null)
      return new URI(DRUID_PREFIX+"//"+host_name+"/"+table_name+"/"+column_names);
    else
      return new URI(DRUID_PREFIX+"//"+host_name+"/"+table_name);
  }

  private void updateKafkaIngestion(Table table) {
    final String overlordAddress = HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_OVERLORD_DEFAULT_ADDRESS);

    final String
        dataSourceName =
        Preconditions.checkNotNull(DruidStorageHandlerUtils.getTableProperty(table, Constants.DRUID_DATA_SOURCE),
            "Druid datasource name is null");

    final String
        kafkaTopic =
        Preconditions.checkNotNull(DruidStorageHandlerUtils.getTableProperty(table,
            DruidConstants.KAFKA_TOPIC), "kafka topic is null");
    final String
        kafkaServers =
        Preconditions.checkNotNull(DruidStorageHandlerUtils.getTableProperty(table,
            DruidConstants.KAFKA_BOOTSTRAP_SERVERS), "kafka connect string is null");

    Properties tableProperties = new Properties();
    tableProperties.putAll(table.getParameters());

    final GranularitySpec granularitySpec = DruidStorageHandlerUtils.getGranularitySpec(getConf(), tableProperties);

    List<FieldSchema> columns = table.getSd().getCols();
    List<String> columnNames = new ArrayList<>(columns.size());
    List<TypeInfo> columnTypes = new ArrayList<>(columns.size());

    for (FieldSchema schema : columns) {
      columnNames.add(schema.getName());
      columnTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(schema.getType()));
    }

    Pair<List<DimensionSchema>, AggregatorFactory[]>
        dimensionsAndAggregates =
        DruidStorageHandlerUtils.getDimensionsAndAggregates(columnNames, columnTypes);
    if (!columnNames.contains(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
      throw new IllegalStateException("Timestamp column (' "
          + DruidConstants.DEFAULT_TIMESTAMP_COLUMN
          + "') not specified in create table; list of columns is : "
          + columnNames);
    }

    DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensionsAndAggregates.lhs, null, null);
    String timestampFormat = DruidStorageHandlerUtils
            .getTableProperty(table, DruidConstants.DRUID_TIMESTAMP_FORMAT);
    String timestampColumnName = DruidStorageHandlerUtils
            .getTableProperty(table, DruidConstants.DRUID_TIMESTAMP_COLUMN);
    if(timestampColumnName == null) {
      timestampColumnName = DruidConstants.DEFAULT_TIMESTAMP_COLUMN;
    }
    final TimestampSpec timestampSpec = new TimestampSpec(timestampColumnName, timestampFormat,
            null
    );
    final InputRowParser inputRowParser = DruidKafkaUtils
            .getInputRowParser(table, timestampSpec, dimensionsSpec);

    final Map<String, Object>
        inputParser =
        JSON_MAPPER.convertValue(inputRowParser, new TypeReference<Map<String, Object>>() {
        });
    final DataSchema
        dataSchema =
        new DataSchema(dataSourceName,
            inputParser,
            dimensionsAndAggregates.rhs,
            granularitySpec,
            null,
            DruidStorageHandlerUtils.JSON_MAPPER);

    IndexSpec indexSpec = DruidStorageHandlerUtils.getIndexSpec(getConf());

    KafkaSupervisorSpec
        spec =
        DruidKafkaUtils.createKafkaSupervisorSpec(table, kafkaTopic, kafkaServers, dataSchema, indexSpec);

    // Fetch existing Ingestion Spec from Druid, if any
    KafkaSupervisorSpec existingSpec = fetchKafkaIngestionSpec(table);
    String
        targetState =
        DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.DRUID_KAFKA_INGESTION);
    if (targetState == null) {
      // Case when user has not specified any ingestion state in the current command
      // if there is a kafka supervisor running then keep it last known state is START otherwise STOP.
      targetState = existingSpec == null ? "STOP" : "START";
    }

    if ("STOP".equalsIgnoreCase(targetState)) {
      if (existingSpec != null) {
        stopKafkaIngestion(overlordAddress, dataSourceName);
      }
    } else if ("START".equalsIgnoreCase(targetState)) {
      if (existingSpec == null || !existingSpec.equals(spec)) {
        DruidKafkaUtils.updateKafkaIngestionSpec(overlordAddress, spec);
      }
    } else if ("RESET".equalsIgnoreCase(targetState)) {
      // Case when there are changes in multiple table properties.
      if (existingSpec != null && !existingSpec.equals(spec)) {
        DruidKafkaUtils.updateKafkaIngestionSpec(overlordAddress, spec);
      }
      resetKafkaIngestion(overlordAddress, dataSourceName);
    } else {
      throw new IllegalArgumentException(String.format(
          "Invalid value for property [%s], Valid values are [START, STOP, RESET]",
          DruidConstants.DRUID_KAFKA_INGESTION));
    }
    // We do not want to keep state in two separate places so remove from hive table properties.
    table.getParameters().remove(DruidConstants.DRUID_KAFKA_INGESTION);
  }

  private void resetKafkaIngestion(String overlordAddress, String dataSourceName) {
    try {
      StringFullResponseHolder
          response =
          RetryUtils.retry(() -> DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
              new Request(HttpMethod.POST, new URL(
                  String.format("http://%s/druid/indexer/v1/supervisor/%s/reset", overlordAddress, dataSourceName))),
              new StringFullResponseHandler(Charset.forName("UTF-8"))), input -> input instanceof IOException,
              getMaxRetryCount());
      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        CONSOLE.printInfo("Druid Kafka Ingestion Reset successful.");
      } else {
        throw new IOException(String.format("Unable to reset Kafka Ingestion Druid status [%d] full response [%s]",
            response.getStatus().getCode(),
            response.getContent()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void stopKafkaIngestion(String overlordAddress, String dataSourceName) {
    try {
      StringFullResponseHolder
          response =
          RetryUtils.retry(() -> DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
              new Request(HttpMethod.POST, new URL(
                  String.format("http://%s/druid/indexer/v1/supervisor/%s/shutdown", overlordAddress, dataSourceName))),
              new StringFullResponseHandler(Charset.forName("UTF-8"))), input -> input instanceof IOException,
              getMaxRetryCount());
      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        CONSOLE.printInfo("Druid Kafka Ingestion shutdown successful.");
      } else {
        throw new IOException(String.format("Unable to stop Kafka Ingestion Druid status [%d] full response [%s]",
            response.getStatus().getCode(),
            response.getContent()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private KafkaSupervisorSpec fetchKafkaIngestionSpec(Table table) {
    // Stop Kafka Ingestion first
    final String
        overlordAddress =
        Preconditions.checkNotNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_OVERLORD_DEFAULT_ADDRESS),
            "Druid Overlord Address is null");
    String
        dataSourceName =
        Preconditions.checkNotNull(DruidStorageHandlerUtils.getTableProperty(table, Constants.DRUID_DATA_SOURCE),
            "Druid Datasource name is null");
    try {
      StringFullResponseHolder
          response =
          RetryUtils.retry(() -> DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
              new Request(HttpMethod.GET,
                  new URL(String.format("http://%s/druid/indexer/v1/supervisor/%s", overlordAddress, dataSourceName))),
              new StringFullResponseHandler(Charset.forName("UTF-8"))), input -> input instanceof IOException,
              getMaxRetryCount());
      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        return JSON_MAPPER.readValue(response.getContent(), KafkaSupervisorSpec.class);
        // Druid Returns 400 Bad Request when not found.
      } else if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND) || response.getStatus()
          .equals(HttpResponseStatus.BAD_REQUEST)) {
        LOG.debug("No Kafka Supervisor found for datasource[%s]", dataSourceName);
        return null;
      } else {
        throw new IOException(String.format(
            "Unable to fetch Kafka Ingestion Spec from Druid status [%d] full response [%s]",
            response.getStatus().getCode(),
            response.getContent()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception while fetching kafka ingestion spec from druid", e);
    }
  }

  /**
   * Fetches kafka supervisor status report from druid overlord. This method will return null if can not fetch report
   *
   * @param table object.
   * @return kafka supervisor report or null when druid overlord is unreachable.
   */
  @Nullable private KafkaSupervisorReport fetchKafkaSupervisorReport(Table table) {
    final String
        overlordAddress =
        Preconditions.checkNotNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_OVERLORD_DEFAULT_ADDRESS),
            "Druid Overlord Address is null");
    final String
        dataSourceName =
        Preconditions.checkNotNull(DruidStorageHandlerUtils.getTableProperty(table, Constants.DRUID_DATA_SOURCE),
            "Druid Datasource name is null");
    try {
      StringFullResponseHolder
          response =
          RetryUtils.retry(() -> DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
              new Request(HttpMethod.GET, new URL(
                  String.format("http://%s/druid/indexer/v1/supervisor/%s/status", overlordAddress, dataSourceName))),
              new StringFullResponseHandler(Charset.forName("UTF-8"))), input -> input instanceof IOException,
              getMaxRetryCount());
      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        return DruidStorageHandlerUtils.JSON_MAPPER.readValue(response.getContent(), KafkaSupervisorReport.class);
        // Druid Returns 400 Bad Request when not found.
      } else if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND) || response.getStatus()
          .equals(HttpResponseStatus.BAD_REQUEST)) {
        LOG.info("No Kafka Supervisor found for datasource[%s]", dataSourceName);
        return null;
      } else {
        LOG.error("Unable to fetch Kafka Supervisor status [%d] full response [%s]",
            response.getStatus().getCode(),
            response.getContent());
        return null;
      }
    } catch (Exception e) {
      LOG.error("Exception while fetching kafka ingestion spec from druid", e);
      return null;
    }
  }

  /**
   * Creates metadata moves then commit the Segment's metadata to Druid metadata store in one TxN.
   *
   * @param table Hive table
   * @param overwrite true if it is an insert overwrite table.
   */
  private List<DataSegment> loadAndCommitDruidSegments(Table table, boolean overwrite, List<DataSegment> segmentsToLoad)
      throws IOException, CallbackFailedException {
    final String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    final String
        segmentDirectory =
        table.getParameters().get(DruidConstants.DRUID_SEGMENT_DIRECTORY) != null ?
            table.getParameters().get(DruidConstants.DRUID_SEGMENT_DIRECTORY) :
            HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY);

    final HdfsDataSegmentPusherConfig hdfsSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    List<DataSegment> publishedDataSegmentList;

    LOG.info(String.format("Moving [%s] Druid segments from staging directory [%s] to Deep storage [%s]",
        segmentsToLoad.size(),
        getStagingWorkingDir().toString(),
        segmentDirectory));
    hdfsSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(hdfsSegmentPusherConfig, getConf(), JSON_MAPPER);
    publishedDataSegmentList =
        DruidStorageHandlerUtils.publishSegmentsAndCommit(getConnector(),
            getDruidMetadataStorageTablesConfig(),
            dataSourceName,
            segmentsToLoad,
            overwrite,
            getConf(),
            dataSegmentPusher);
    return publishedDataSegmentList;
  }

  /**
   * This function checks the load status of Druid segments by polling druid coordinator.
   * @param segments List of druid segments to check for
   */
  private void checkLoadStatus(List<DataSegment> segments) {
    final String
        coordinatorAddress =
        HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_COORDINATOR_DEFAULT_ADDRESS);
    int maxTries = getMaxRetryCount();

    LOG.debug("checking load status from coordinator {}", coordinatorAddress);

    String coordinatorResponse;
    try {
      coordinatorResponse =
          RetryUtils.retry(() -> DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
              new Request(HttpMethod.GET, new URL(String.format("http://%s/status", coordinatorAddress))),
              new StringFullResponseHandler(Charset.forName("UTF-8"))).getContent(),
              input -> input instanceof IOException, maxTries);
    } catch (Exception e) {
      CONSOLE.printInfo("Will skip waiting for data loading, coordinator unavailable");
      return;
    }
    if (Strings.isNullOrEmpty(coordinatorResponse)) {
      CONSOLE.printInfo("Will skip waiting for data loading empty response from coordinator");
    }
    CONSOLE.printInfo(String.format("Waiting for the loading of [%s] segments", segments.size()));
    long passiveWaitTimeMs = HiveConf.getLongVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_PASSIVE_WAIT_TIME);
    Set<URL> urlsOfUnloadedSegments = segments
        .stream()
        .map(dataSegment -> {
          try {
            //Need to make sure that we are using segment identifier
            return new URL(String.format("http://%s/druid/coordinator/v1/datasources/%s/segments/%s",
                coordinatorAddress,
                dataSegment.getDataSource(),
                dataSegment.getId().toString()));
          } catch (MalformedURLException e) {
            Throwables.propagate(e);
          }
          return null;
        })
        .collect(Collectors.toSet());

    int numRetries = 0;
    while (numRetries++ < maxTries && !urlsOfUnloadedSegments.isEmpty()) {
      urlsOfUnloadedSegments = ImmutableSet.copyOf(Sets.filter(urlsOfUnloadedSegments, input -> {
        try {
          String result = DruidStorageHandlerUtils
              .getResponseFromCurrentLeader(getHttpClient(), new Request(HttpMethod.GET, input),
                  new StringFullResponseHandler(Charset.forName("UTF-8"))).getContent();

          LOG.debug("Checking segment [{}] response is [{}]", input, result);
          return Strings.isNullOrEmpty(result);
        } catch (InterruptedException | ExecutionException e) {
          LOG.error(String.format("Error while checking URL [%s]", input), e);
          return true;
        }
      }));

      try {
        if (!urlsOfUnloadedSegments.isEmpty()) {
          Thread.sleep(passiveWaitTimeMs);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if (!urlsOfUnloadedSegments.isEmpty()) {
      // We are not Throwing an exception since it might be a transient issue that is blocking loading
      CONSOLE.printError(String.format("Wait time exhausted and we have [%s] out of [%s] segments not loaded yet",
          urlsOfUnloadedSegments.size(),
          segments.size()));
    }
  }

  @VisibleForTesting void deleteSegment(DataSegment segment) throws SegmentLoadingException {

    final Path path = DruidStorageHandlerUtils.getPath(segment);
    LOG.info("removing segment {}, located at path {}", segment.getId().toString(), path);

    try {
      if (path.getName().endsWith(".zip")) {

        final FileSystem fs = path.getFileSystem(getConf());

        if (!fs.exists(path)) {
          LOG.warn("Segment Path {} does not exist. It appears to have been deleted already.", path);
          return;
        }

        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        Path partitionNumDir = path.getParent();
        if (!fs.delete(partitionNumDir, true)) {
          throw new SegmentLoadingException("Unable to kill segment, failed to delete dir [%s]",
              partitionNumDir.toString());
        }

        //try to delete other directories if possible
        Path versionDir = partitionNumDir.getParent();
        if (safeNonRecursiveDelete(fs, versionDir)) {
          Path intervalDir = versionDir.getParent();
          if (safeNonRecursiveDelete(fs, intervalDir)) {
            Path dataSourceDir = intervalDir.getParent();
            safeNonRecursiveDelete(fs, dataSourceDir);
          }
        }
      } else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    } catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  private static boolean safeNonRecursiveDelete(FileSystem fs, Path path) {
    try {
      return fs.delete(path, false);
    } catch (Exception ex) {
      return false;
    }
  }

  @Override public void preDropTable(Table table) {
    // Nothing to do
  }

  @Override public void rollbackDropTable(Table table) {
    // Nothing to do
  }

  @Override public void commitDropTable(Table table, boolean deleteData) {
    if (DruidKafkaUtils.isKafkaStreamingTable(table)) {
      // Stop Kafka Ingestion first
      final String
          overlordAddress =
          Preconditions.checkNotNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_OVERLORD_DEFAULT_ADDRESS),
              "Druid Overlord Address is null");
      String
          dataSourceName =
          Preconditions.checkNotNull(DruidStorageHandlerUtils.getTableProperty(table, Constants.DRUID_DATA_SOURCE),
              "Druid Datasource name is null");
      stopKafkaIngestion(overlordAddress, dataSourceName);
    }

    String
        dataSourceName =
        Preconditions.checkNotNull(table.getParameters().get(Constants.DRUID_DATA_SOURCE), "DataSource name is null !");
    // Move MetaStoreUtils.isExternalTablePurge(table) calls to a common place for all StorageHandlers
    // deleteData flag passed down to StorageHandler should be true only if
    // MetaStoreUtils.isExternalTablePurge(table) returns true.
    if (deleteData && MetaStoreUtils.isExternalTablePurge(table)) {
      LOG.info("Dropping with purge all the data for data source {}", dataSourceName);
      List<DataSegment>
          dataSegmentList =
          DruidStorageHandlerUtils.getDataSegmentList(getConnector(),
              getDruidMetadataStorageTablesConfig(),
              dataSourceName);
      if (dataSegmentList.isEmpty()) {
        LOG.info("Nothing to delete for data source {}", dataSourceName);
        return;
      }
      for (DataSegment dataSegment : dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        } catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while deleting segment [%s]", dataSegment.getId().toString()), e);
        }
      }
    }
    if (DruidStorageHandlerUtils.disableDataSource(getConnector(),
        getDruidMetadataStorageTablesConfig(),
        dataSourceName)) {
      LOG.info("Successfully dropped druid data source {}", dataSourceName);
    }
  }

  @Override public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    LOG.debug("commit insert into table {} overwrite {}", table.getTableName(), overwrite);
    try {
      // Check if there segments to load
      final Path segmentDescriptorDir = getSegmentDescriptorDir();
      final List<DataSegment> segmentsToLoad = fetchSegmentsMetadata(segmentDescriptorDir);
      final String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
      //No segments to load still need to honer overwrite
      if (segmentsToLoad.isEmpty() && overwrite) {
        //disable datasource
        //Case it is an insert overwrite we have to disable the existing Druid DataSource
        DruidStorageHandlerUtils.disableDataSource(getConnector(),
            getDruidMetadataStorageTablesConfig(),
            dataSourceName);
      } else if (!segmentsToLoad.isEmpty()) {
        // at this point we have Druid segments from reducers but we need to atomically
        // rename and commit to metadata
        // Moving Druid segments and committing to druid metadata as one transaction.
        checkLoadStatus(loadAndCommitDruidSegments(table, overwrite, segmentsToLoad));
      }
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    } catch (CallbackFailedException c) {
      LOG.error("Error while committing transaction to druid metadata storage", c);
      throw new MetaException(c.getCause().getMessage());
    } finally {
      cleanWorkingDir();
    }
  }

  private List<DataSegment> fetchSegmentsMetadata(Path segmentDescriptorDir) throws IOException {
    if (!segmentDescriptorDir.getFileSystem(getConf()).exists(segmentDescriptorDir)) {
      LOG.info("Directory {} does not exist, ignore this if it is create statement or inserts of 0 rows,"
              + " no Druid segments to move, cleaning working directory {}",
          segmentDescriptorDir.toString(),
          getStagingWorkingDir().toString());
      return Collections.emptyList();
    }
    return DruidStorageHandlerUtils.getCreatedSegments(segmentDescriptorDir, getConf());
  }

  @Override public void preInsertTable(Table table, boolean overwrite) {

  }

  @Override public void rollbackInsertTable(Table table, boolean overwrite) {
    // do nothing
  }

  @Override public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    jobProperties.put(Constants.DRUID_DATA_SOURCE, tableDesc.getTableName());
    jobProperties.put(DruidConstants.DRUID_SEGMENT_VERSION, new DateTime().toString());
    jobProperties.put(DruidConstants.DRUID_JOB_WORKING_DIRECTORY, getStagingWorkingDir().toString());
    // DruidOutputFormat will write segments in an intermediate directory
    jobProperties.put(DruidConstants.DRUID_SEGMENT_INTERMEDIATE_DIRECTORY,
        getIntermediateSegmentDir().toString());
  }

  @Override public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    final boolean kerberosEnabled = HiveConf.getBoolVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_KERBEROS_ENABLE);
    if (kerberosEnabled && UserGroupInformation.isSecurityEnabled()) {
      // AM can not do Kerberos Auth so will do the input split generation in the HS2
      LOG.debug("Setting {} to {} to enable split generation on HS2",
          HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(),
          Boolean.FALSE.toString());
      jobConf.set(HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(), Boolean.FALSE.toString());
    }
    try {
      Utilities.addDependencyJars(jobConf, DruidRecordWriter.class);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public LockType getLockType(WriteEntity writeEntity) {
    if (writeEntity.getWriteType().equals(WriteEntity.WriteType.INSERT)) {
      return LockType.SHARED_READ;
    }
    return LockType.SHARED_WRITE;
  }

  @Override public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

  private String getUniqueId() {
    if (uniqueId == null) {
      uniqueId =
          Preconditions.checkNotNull(Strings.emptyToNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_QUERY_ID)),
              "Hive query id is null");
    }
    return uniqueId;
  }

  private Path getStagingWorkingDir() {
    return new Path(getRootWorkingDir(), makeStagingName());
  }

  private MetadataStorageTablesConfig getDruidMetadataStorageTablesConfig() {
    if (druidMetadataStorageTablesConfig != null) {
      return druidMetadataStorageTablesConfig;
    }
    final String base = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_BASE);
    druidMetadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase(base);
    return druidMetadataStorageTablesConfig;
  }

  private SQLMetadataConnector getConnector() {
    return Suppliers.memoize(this::buildConnector).get();
  }

  private static String getPassword(Configuration conf, HiveConf.ConfVars var) {
    try {
      final char[] password = conf.getPassword(var.varname);
      return password == null ? null : String.valueOf(password);
    } catch (IOException e) {
      // Reading password from configuration for backwards compatibility.
      LOG.warn("Unable to retrieve password from credential providers. Trying to read it from config..", e);
      return conf.get(var.varname);
    }
  }

  private SQLMetadataConnector buildConnector() {

    if (connector != null) {
      return connector;
    }

    final String dbType = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_TYPE);
    final String username = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_USERNAME);
    final String password = getPassword(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_PASSWORD);
    final String uri = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_URI);
    LOG.debug("Supplying SQL Connector with DB type {}, URI {}, User {}", dbType, uri, username);
    @SuppressWarnings("Guava") final Supplier<MetadataStorageConnectorConfig>
        storageConnectorConfigSupplier =
        Suppliers.ofInstance(new MetadataStorageConnectorConfig() {
          @Override public String getConnectURI() {
            return uri;
          }

          @Override public String getUser() {
            return Strings.emptyToNull(username);
          }

          @Override public String getPassword() {
            return Strings.emptyToNull(password);
          }
        });
    switch (dbType) {
    case "mysql":
      connector =
          new MySQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig()),
              new MySQLConnectorConfig());
      break;
    case "postgresql":
      connector =
          new PostgreSQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig()), new PostgreSQLConnectorConfig(),
              new PostgreSQLTablesConfig());

      break;
    case "derby":
      connector =
          new DerbyConnector(new DerbyMetadataStorage(storageConnectorConfigSupplier.get()),
              storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig()));
      break;
    default:
      throw new IllegalStateException(String.format("Unknown metadata storage type [%s]", dbType));
    }
    return connector;
  }

  @VisibleForTesting String makeStagingName() {
    return ".staging-".concat(getUniqueId().replace(":", ""));
  }

  private Path getSegmentDescriptorDir() {
    return new Path(getStagingWorkingDir(), SEGMENTS_DESCRIPTOR_DIR_NAME);
  }

  private Path getIntermediateSegmentDir() {
    return new Path(getStagingWorkingDir(), INTERMEDIATE_SEGMENT_DIR_NAME);
  }

  private void cleanWorkingDir() {
    final FileSystem fileSystem;
    try {
      fileSystem = getStagingWorkingDir().getFileSystem(getConf());
      fileSystem.delete(getStagingWorkingDir(), true);
    } catch (IOException e) {
      LOG.error("Got Exception while cleaning working directory", e);
    }
  }

  private String getRootWorkingDir() {
    if (Strings.isNullOrEmpty(rootWorkingDir)) {
      rootWorkingDir = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_WORKING_DIR);
    }
    return rootWorkingDir;
  }

  private static HttpClient makeHttpClient(Lifecycle lifecycle) {
    final int
        numConnection =
        HiveConf.getIntVar(SessionState.getSessionConf(), HiveConf.ConfVars.HIVE_DRUID_NUM_HTTP_CONNECTION);
    final Period
        readTimeout =
        new Period(HiveConf.getVar(SessionState.getSessionConf(), HiveConf.ConfVars.HIVE_DRUID_HTTP_READ_TIMEOUT));
    LOG.info("Creating Druid HTTP client with {} max parallel connections and {}ms read timeout",
        numConnection,
        readTimeout.toStandardDuration().getMillis());

    final HttpClient
        httpClient =
        HttpClientInit.createClient(HttpClientConfig.builder()
            .withNumConnections(numConnection)
            .withReadTimeout(new Period(readTimeout).toStandardDuration())
            .build(), lifecycle);
    final boolean kerberosEnabled =
        HiveConf.getBoolVar(SessionState.getSessionConf(), HiveConf.ConfVars.HIVE_DRUID_KERBEROS_ENABLE);
    if (kerberosEnabled && UserGroupInformation.isSecurityEnabled()) {
      LOG.info("building Kerberos Http Client");
      return new KerberosHttpClient(httpClient);
    }
    return httpClient;
  }

  public static HttpClient getHttpClient() {
    return HTTP_CLIENT;
  }

  @Override public void preAlterTable(Table table, EnvironmentContext context) throws MetaException {
    String alterOpType = context == null ? null : context.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
    // alterOpType is null in case of stats update
    if (alterOpType != null && !ALLOWED_ALTER_TYPES.contains(alterOpType)) {
      throw new MetaException("ALTER TABLE can not be used for " + alterOpType + " to a non-native table ");
    }
    if (DruidKafkaUtils.isKafkaStreamingTable(table)) {
      updateKafkaIngestion(table);
    }
  }

  private int getMaxRetryCount() {
    return HiveConf.getIntVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_MAX_TRIES);
  }

  @Override public StorageHandlerInfo getStorageHandlerInfo(Table table) throws MetaException {
    if (DruidKafkaUtils.isKafkaStreamingTable(table)) {
      KafkaSupervisorReport kafkaSupervisorReport = fetchKafkaSupervisorReport(table);
      if (kafkaSupervisorReport == null) {
        return DruidStorageHandlerInfo.UNREACHABLE;
      }
      return new DruidStorageHandlerInfo(kafkaSupervisorReport);
    } else {
      // Currently we do not expose any runtime info for non-streaming tables.
      // In future extend this add more information regarding table status.
      // e.g. Total size of segments in druid, load status of table on historical nodes etc.
      return null;
    }
  }

  @Override public Map<String, String> getOperatorDescProperties(OperatorDesc operatorDesc,
      Map<String, String> initialProps) {
    if (operatorDesc instanceof TableScanDesc) {
      TableScanDesc tableScanDesc = (TableScanDesc) operatorDesc;
      ExprNodeGenericFuncDesc filterExpr = tableScanDesc.getFilterExpr();
      String druidQuery = initialProps.get(Constants.DRUID_QUERY_JSON);

      if (filterExpr != null && druidQuery != null) {
        try {
          Query query = DruidStorageHandlerUtils.JSON_MAPPER.readValue(druidQuery, BaseQuery.class);
          Query queryWithDynamicFilters = DruidStorageHandlerUtils.addDynamicFilters(query, filterExpr, conf, false);
          Map<String, String> props = Maps.newHashMap(initialProps);
          props.put(Constants.DRUID_QUERY_JSON,
              DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(queryWithDynamicFilters));
          return props;
        } catch (IOException e) {
          LOG.error("Exception while deserializing druid query. Explain plan may not have final druid query", e);
        }
      }
    }
    // Case when we do not have any additional info to add.
    return initialProps;
  }
}
