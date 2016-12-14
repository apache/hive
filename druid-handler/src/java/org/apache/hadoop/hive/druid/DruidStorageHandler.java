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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.mysql.MySQLConnector;
import io.druid.metadata.storage.postgresql.PostgreSQLConnector;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class DruidStorageHandler extends DefaultStorageHandler implements HiveMetaHook {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  public static final String SEGMENTS_DESCRIPTOR_DIR_NAME = "segmentsDescriptorDir";

  private final SQLMetadataConnector connector;

  private final SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler;

  private final MetadataStorageTablesConfig druidMetadataStorageTablesConfig;

  private String uniqueId = null;

  private String rootWorkingDir = null;

  public DruidStorageHandler() {
    //this is the default value in druid
    final String base = HiveConf
            .getVar(SessionState.getSessionConf(), HiveConf.ConfVars.DRUID_METADATA_BASE);
    final String dbType = HiveConf
            .getVar(SessionState.getSessionConf(), HiveConf.ConfVars.DRUID_METADATA_DB_TYPE);
    final String username = HiveConf
            .getVar(SessionState.getSessionConf(), HiveConf.ConfVars.DRUID_METADATA_DB_USERNAME);
    final String password = HiveConf
            .getVar(SessionState.getSessionConf(), HiveConf.ConfVars.DRUID_METADATA_DB_PASSWORD);
    final String uri = HiveConf
            .getVar(SessionState.getSessionConf(), HiveConf.ConfVars.DRUID_METADATA_DB_URI);
    druidMetadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase(base);

    final Supplier<MetadataStorageConnectorConfig> storageConnectorConfigSupplier = Suppliers.<MetadataStorageConnectorConfig>ofInstance(
            new MetadataStorageConnectorConfig() {
              @Override
              public String getConnectURI() {
                return uri;
              }

              @Override
              public String getUser() {
                return username;
              }

              @Override
              public String getPassword() {
                return password;
              }
            });

    if (dbType.equals("mysql")) {
      connector = new MySQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(druidMetadataStorageTablesConfig)
      );
    } else if (dbType.equals("postgres")) {
      connector = new PostgreSQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(druidMetadataStorageTablesConfig)
      );
    } else {
      throw new IllegalStateException(String.format("Unknown metadata storage type [%s]", dbType));
    }
    druidSqlMetadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(connector);
  }

  @VisibleForTesting
  public DruidStorageHandler(SQLMetadataConnector connector,
          SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler,
          MetadataStorageTablesConfig druidMetadataStorageTablesConfig
  ) {
    this.connector = connector;
    this.druidSqlMetadataStorageUpdaterJobHandler = druidSqlMetadataStorageUpdaterJobHandler;
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
      return;
    }
    // If it is not an external table we need to check the metadata
    try {
      connector.createSegmentTable();
    } catch (Exception e) {
      LOG.error("Exception while trying to create druid segments table", e);
      throw new MetaException(e.getMessage());
    }
    Collection<String> existingDataSources = DruidStorageHandlerUtils
            .getAllDataSourceNames(connector, druidMetadataStorageTablesConfig);
    LOG.debug(String.format("pre-create data source with name [%s]", dataSourceName));
    if (existingDataSources.contains(dataSourceName)) {
      throw new MetaException(String.format("Data source [%s] already existing", dataSourceName));
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    if (MetaStoreUtils.isExternalTable(table)) {
      return;
    }
    final Path segmentDescriptorDir = getSegmentDescriptorDir();
    try {
      List<DataSegment> dataSegmentList = DruidStorageHandlerUtils
              .getPublishedSegments(segmentDescriptorDir, getConf());
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
      return;
    }
    LOG.info(String.format("Committing table [%s] to the druid metastore", table.getDbName()));
    final Path tableDir = getSegmentDescriptorDir();
    try {
      List<DataSegment> segmentList = DruidStorageHandlerUtils
              .getPublishedSegments(tableDir, getConf());
      LOG.info(String.format("Found [%d] segments under path [%s]", segmentList.size(), tableDir));
      druidSqlMetadataStorageUpdaterJobHandler.publishSegments(
              druidMetadataStorageTablesConfig.getSegmentsTable(),
              segmentList,
              DruidStorageHandlerUtils.JSON_MAPPER
      );
    } catch (IOException e) {
      LOG.error("Exception while commit", e);
      Throwables.propagate(e);
    } finally {
      cleanWorkingDir();
    }
  }

  @VisibleForTesting
  protected void deleteSegment(DataSegment segment) throws SegmentLoadingException {

    final Path path = getPath(segment);
    LOG.info(String.format("removing segment[%s], located at path[%s]", segment.getIdentifier(),
            path
    ));

    try {
      if (path.getName().endsWith(".zip")) {

        final FileSystem fs = path.getFileSystem(getConf());

        if (!fs.exists(path)) {
          LOG.warn(String.format(
                  "Segment Path [%s] does not exist. It appears to have been deleted already.",
                  path
          ));
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

  private static Path getPath(DataSegment dataSegment) {
    return new Path(String.valueOf(dataSegment.getLoadSpec().get("path")));
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
      LOG.info(String.format("Dropping with purge all the data for data source [%s]",
              dataSourceName
      ));
      List<DataSegment> dataSegmentList = DruidStorageHandlerUtils
              .getDataSegmentList(connector, druidMetadataStorageTablesConfig, dataSourceName);
      if (dataSegmentList.isEmpty()) {
        LOG.info(String.format("Nothing to delete for data source [%s]", dataSourceName));
        return;
      }
      for (DataSegment dataSegment : dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        } catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while deleting segment [%s]", dataSegment.getIdentifier()),
                  e
          );
        }
      }
    }
    if (DruidStorageHandlerUtils
            .disableDataSource(connector, druidMetadataStorageTablesConfig, dataSourceName)) {
      LOG.info(String.format("Successfully dropped druid data source [%s]", dataSourceName));
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {
    jobProperties.put(Constants.DRUID_SEGMENT_VERSION, new DateTime().toString());
    jobProperties.put(Constants.DRUID_JOB_WORKING_DIRECTORY, getStagingWorkingDir().toString());
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

  @VisibleForTesting
  protected String makeStagingName() {
    return ".staging-".concat(getUniqueId().replace(":", ""));
  }

  private Path getSegmentDescriptorDir() {
    return new Path(getStagingWorkingDir(), SEGMENTS_DESCRIPTOR_DIR_NAME);
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
}
