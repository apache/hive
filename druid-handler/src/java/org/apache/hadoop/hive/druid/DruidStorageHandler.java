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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.RetryUtils;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.derby.DerbyConnector;
import io.druid.metadata.storage.derby.DerbyMetadataStorage;
import io.druid.metadata.storage.mysql.MySQLConnector;
import io.druid.metadata.storage.postgresql.PostgreSQLConnector;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.DruidRecordWriter;
import org.apache.hadoop.hive.druid.security.KerberosHttpClient;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({ "rawtypes" })
public class DruidStorageHandler extends DefaultHiveMetaHook implements HiveStorageHandler {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  protected static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);

  public static final String SEGMENTS_DESCRIPTOR_DIR_NAME = "segmentsDescriptorDir";

  public static final String INTERMEDIATE_SEGMENT_DIR_NAME = "intermediateSegmentDir";

  private static final HttpClient HTTP_CLIENT;

  static {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      lifecycle.start();
    } catch (Exception e) {
      LOG.error("Issues with lifecycle start", e);
    }
    HTTP_CLIENT = makeHttpClient(lifecycle);
    ShutdownHookManager.addShutdownHook(() -> lifecycle.stop());
  }

  private SQLMetadataConnector connector;

  private MetadataStorageTablesConfig druidMetadataStorageTablesConfig = null;

  private String uniqueId = null;

  private String rootWorkingDir = null;

  private Configuration conf;

  public DruidStorageHandler() {
  }

  @VisibleForTesting
  public DruidStorageHandler(SQLMetadataConnector connector,
          MetadataStorageTablesConfig druidMetadataStorageTablesConfig
  ) {
    this.connector = connector;
    this.druidMetadataStorageTablesConfig = druidMetadataStorageTablesConfig;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DruidOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {

  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> jobSecrets
  ) {

  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // Do safety checks
    if (MetaStoreUtils.isExternalTable(table) && !StringUtils
            .isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid");
    }

    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
    String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    if (MetaStoreUtils.isExternalTable(table)) {
      if (dataSourceName == null) {
        throw new MetaException(
            String.format("Datasource name should be specified using [%s] for external tables "
                + "using Druid", Constants.DRUID_DATA_SOURCE));
      }
      // If it is an external table, we are done
      return;
    }
    // It is not an external table
    // We need to check that datasource was not specified by user
    if (dataSourceName != null) {
      throw new MetaException(
          String.format("Datasource name cannot be specified using [%s] for managed tables "
              + "using Druid", Constants.DRUID_DATA_SOURCE));
    }
    // We need to check the Druid metadata
    dataSourceName = Warehouse.getQualifiedName(table);
    try {
      getConnector().createSegmentTable();
    } catch (Exception e) {
      LOG.error("Exception while trying to create druid segments table", e);
      throw new MetaException(e.getMessage());
    }
    Collection<String> existingDataSources = DruidStorageHandlerUtils
            .getAllDataSourceNames(getConnector(), getDruidMetadataStorageTablesConfig());
    LOG.debug("pre-create data source with name {}", dataSourceName);
    if (existingDataSources.contains(dataSourceName)) {
      throw new MetaException(String.format("Data source [%s] already existing", dataSourceName));
    }
    table.getParameters().put(Constants.DRUID_DATA_SOURCE, dataSourceName);
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    if (MetaStoreUtils.isExternalTable(table)) {
      return;
    }
    final Path segmentDescriptorDir = getSegmentDescriptorDir();
    try {
      List<DataSegment> dataSegmentList = DruidStorageHandlerUtils
              .getCreatedSegments(segmentDescriptorDir, getConf());
      for (DataSegment dataSegment : dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        } catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while trying to clean the segment [%s]", dataSegment), e);
        }
      }
    } catch (IOException e) {
      LOG.error("Exception while rollback", e);
      throw Throwables.propagate(e);
    } finally {
      cleanWorkingDir();
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    if (MetaStoreUtils.isExternalTable(table)) {
      // For external tables, we do not need to do anything else
      return;
    }
    loadDruidSegments(table, true);
  }


  protected void loadDruidSegments(Table table, boolean overwrite) throws MetaException {
    // at this point we have Druid segments from reducers but we need to atomically
    // rename and commit to metadata
    final String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    final List<DataSegment> segmentList = Lists.newArrayList();
    final Path tableDir = getSegmentDescriptorDir();
    // Read the created segments metadata from the table staging directory
    try {
      segmentList.addAll(DruidStorageHandlerUtils.getCreatedSegments(tableDir, getConf()));
    } catch (IOException e) {
      LOG.error("Failed to load segments descriptor from directory {}", tableDir.toString());
      Throwables.propagate(e);
      cleanWorkingDir();
    }
    // Moving Druid segments and committing to druid metadata as one transaction.
    final HdfsDataSegmentPusherConfig hdfsSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    List<DataSegment> publishedDataSegmentList = Lists.newArrayList();
    final String segmentDirectory =
            table.getParameters().get(Constants.DRUID_SEGMENT_DIRECTORY) != null
                    ? table.getParameters().get(Constants.DRUID_SEGMENT_DIRECTORY)
                    : HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY);
    LOG.info(String.format(
            "Moving [%s] Druid segments from staging directory [%s] to Deep storage [%s]",
            segmentList.size(),
            getStagingWorkingDir(),
            segmentDirectory

            ));
    hdfsSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    try {
      DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(hdfsSegmentPusherConfig,
              getConf(),
              DruidStorageHandlerUtils.JSON_MAPPER
      );
      publishedDataSegmentList = DruidStorageHandlerUtils.publishSegmentsAndCommit(
              getConnector(),
              getDruidMetadataStorageTablesConfig(),
              dataSourceName,
              segmentList,
              overwrite,
              getConf(),
              dataSegmentPusher
      );

    } catch (CallbackFailedException | IOException e) {
      LOG.error("Failed to move segments from staging directory");
      if (e instanceof CallbackFailedException) {
        Throwables.propagate(e.getCause());
      }
      Throwables.propagate(e);
    } finally {
      cleanWorkingDir();
    }
      checkLoadStatus(publishedDataSegmentList);
  }

  /**
   * This function checks the load status of Druid segments by polling druid coordinator.
   * @param segments List of druid segments to check for
   *
   * @return count of yet to load segments.
   */
  private int checkLoadStatus(List<DataSegment> segments){
    final String coordinatorAddress = HiveConf
            .getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_COORDINATOR_DEFAULT_ADDRESS);
    int maxTries = HiveConf.getIntVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_MAX_TRIES);
    if (maxTries == 0) {
      return segments.size();
    }
    LOG.debug("checking load status from coordinator {}", coordinatorAddress);

    String coordinatorResponse;
    try {
      coordinatorResponse = RetryUtils.retry(() -> DruidStorageHandlerUtils.getURL(getHttpClient(),
              new URL(String.format("http://%s/status", coordinatorAddress))
      ), input -> input instanceof IOException, maxTries);
    } catch (Exception e) {
      console.printInfo(
              "Will skip waiting for data loading, coordinator unavailable");
      return segments.size();
    }
    if (Strings.isNullOrEmpty(coordinatorResponse)) {
      console.printInfo(
              "Will skip waiting for data loading empty response from coordinator");
      return segments.size();
    }
    console.printInfo(
            String.format("Waiting for the loading of [%s] segments", segments.size()));
    long passiveWaitTimeMs = HiveConf
            .getLongVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_PASSIVE_WAIT_TIME);
    Set<URL> UrlsOfUnloadedSegments = segments.stream().map(dataSegment -> {
      try {
        //Need to make sure that we are using segment identifier
        return new URL(String.format("http://%s/druid/coordinator/v1/datasources/%s/segments/%s",
                coordinatorAddress, dataSegment.getDataSource(), dataSegment.getIdentifier()
        ));
      } catch (MalformedURLException e) {
        Throwables.propagate(e);
      }
      return null;
    }).collect(Collectors.toSet());

    int numRetries = 0;
    while (numRetries++ < maxTries && !UrlsOfUnloadedSegments.isEmpty()) {
      UrlsOfUnloadedSegments = ImmutableSet.copyOf(Sets.filter(UrlsOfUnloadedSegments, input -> {
        try {
          String result = DruidStorageHandlerUtils.getURL(getHttpClient(), input);
          LOG.debug("Checking segment [{}] response is [{}]", input, result);
          return Strings.isNullOrEmpty(result);
        } catch (IOException e) {
          LOG.error(String.format("Error while checking URL [%s]", input), e);
          return true;
        }
      }));

      try {
        if (!UrlsOfUnloadedSegments.isEmpty()) {
          Thread.sleep(passiveWaitTimeMs);
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        Throwables.propagate(e);
      }
    }
    if (!UrlsOfUnloadedSegments.isEmpty()) {
      // We are not Throwing an exception since it might be a transient issue that is blocking loading
      console.printError(String.format(
              "Wait time exhausted and we have [%s] out of [%s] segments not loaded yet",
              UrlsOfUnloadedSegments.size(), segments.size()
      ));
    }
    return UrlsOfUnloadedSegments.size();
  }

  @VisibleForTesting
  protected void deleteSegment(DataSegment segment) throws SegmentLoadingException {

    final Path path = DruidStorageHandlerUtils.getPath(segment);
    LOG.info("removing segment {}, located at path {}", segment.getIdentifier(), path);

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
          throw new SegmentLoadingException(
                  "Unable to kill segment, failed to delete dir [%s]",
                  partitionNumDir.toString()
          );
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

  @Override
  public void preDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    if (MetaStoreUtils.isExternalTable(table)) {
      return;
    }
    String dataSourceName = Preconditions
            .checkNotNull(table.getParameters().get(Constants.DRUID_DATA_SOURCE),
                    "DataSource name is null !"
            );

    if (deleteData == true) {
      LOG.info("Dropping with purge all the data for data source {}", dataSourceName);
      List<DataSegment> dataSegmentList = DruidStorageHandlerUtils
              .getDataSegmentList(getConnector(), getDruidMetadataStorageTablesConfig(), dataSourceName);
      if (dataSegmentList.isEmpty()) {
        LOG.info("Nothing to delete for data source {}", dataSourceName);
        return;
      }
      for (DataSegment dataSegment : dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        } catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while deleting segment [%s]", dataSegment.getIdentifier()), e);
        }
      }
    }
    if (DruidStorageHandlerUtils
            .disableDataSource(getConnector(), getDruidMetadataStorageTablesConfig(), dataSourceName)) {
      LOG.info("Successfully dropped druid data source {}", dataSourceName);
    }
  }

  @Override
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    LOG.debug("commit insert into table {} overwrite {}", table.getTableName(),
            overwrite);
    if (MetaStoreUtils.isExternalTable(table)) {
      throw new MetaException("Cannot insert data into external table backed by Druid");
    }
    this.loadDruidSegments(table, overwrite);
  }

  @Override
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {

  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    // do nothing
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    jobProperties.put(Constants.DRUID_DATA_SOURCE, tableDesc.getTableName());
    jobProperties.put(Constants.DRUID_SEGMENT_VERSION, new DateTime().toString());
    jobProperties.put(Constants.DRUID_JOB_WORKING_DIRECTORY, getStagingWorkingDir().toString());
    // DruidOutputFormat will write segments in an intermediate directory
    jobProperties.put(Constants.DRUID_SEGMENT_INTERMEDIATE_DIRECTORY,
            getIntermediateSegmentDir().toString());
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    if (UserGroupInformation.isSecurityEnabled()) {
      // AM can not do Kerberos Auth so will do the input split generation in the HS2
      LOG.debug("Setting {} to {} to enable split generation on HS2", HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(),
              Boolean.FALSE.toString()
      );
      jobConf.set(HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(), Boolean.FALSE.toString());
    }
    try {
      DruidStorageHandlerUtils.addDependencyJars(jobConf, DruidRecordWriter.class);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

  public String getUniqueId() {
    if (uniqueId == null) {
      uniqueId = Preconditions.checkNotNull(
              Strings.emptyToNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVEQUERYID)),
              "Hive query id is null"
      );
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
    final String base = HiveConf
            .getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_BASE);
    druidMetadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase(base);
    return druidMetadataStorageTablesConfig;
  }

  private SQLMetadataConnector getConnector() {
    if (connector != null) {
      return connector;
    }

    final String dbType = HiveConf
            .getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_TYPE);
    final String username = HiveConf
            .getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_USERNAME);
    final String password = HiveConf
            .getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_PASSWORD);
    final String uri = HiveConf
            .getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_URI);


    final Supplier<MetadataStorageConnectorConfig> storageConnectorConfigSupplier = Suppliers.<MetadataStorageConnectorConfig>ofInstance(
            new MetadataStorageConnectorConfig() {
              @Override
              public String getConnectURI() {
                return uri;
              }

              @Override
              public String getUser() {
                return Strings.emptyToNull(username);
              }

              @Override
              public String getPassword() {
                return Strings.emptyToNull(password);
              }
            });
    if (dbType.equals("mysql")) {
      connector = new MySQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig())
      );
    } else if (dbType.equals("postgresql")) {
      connector = new PostgreSQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig())
      );

    } else if (dbType.equals("derby")) {
      connector = new DerbyConnector(new DerbyMetadataStorage(storageConnectorConfigSupplier.get()),
              storageConnectorConfigSupplier, Suppliers.ofInstance(getDruidMetadataStorageTablesConfig())
      );
    }
    else {
      throw new IllegalStateException(String.format("Unknown metadata storage type [%s]", dbType));
    }

    return connector;
  }

  @VisibleForTesting
  protected String makeStagingName() {
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
    final int numConnection = HiveConf
            .getIntVar(SessionState.getSessionConf(),
                    HiveConf.ConfVars.HIVE_DRUID_NUM_HTTP_CONNECTION
            );
    final Period readTimeout = new Period(
            HiveConf.getVar(SessionState.getSessionConf(),
                    HiveConf.ConfVars.HIVE_DRUID_HTTP_READ_TIMEOUT
            ));
    LOG.info("Creating Druid HTTP client with {} max parallel connections and {}ms read timeout",
            numConnection, readTimeout.toStandardDuration().getMillis()
    );

    final HttpClient httpClient = HttpClientInit.createClient(
            HttpClientConfig.builder().withNumConnections(numConnection)
                    .withReadTimeout(new Period(readTimeout).toStandardDuration()).build(),
            lifecycle
    );
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("building Kerberos Http Client");
      return new KerberosHttpClient(httpClient);
    }
    return httpClient;
  }

  public static HttpClient getHttpClient() {
    return HTTP_CLIENT;
  }
}
