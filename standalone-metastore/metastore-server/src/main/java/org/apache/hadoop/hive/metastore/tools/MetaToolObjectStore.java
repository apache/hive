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

package org.apache.hadoop.hive.metastore.tools;

import javax.jdo.Query;
import javax.jdo.datastore.JDOConnection;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.hadoop.hive.metastore.Batchable.runBatched;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * This class should be used in metatool only
 */
public class MetaToolObjectStore extends ObjectStore {

  private static final Logger LOG = LoggerFactory.getLogger(MetaToolObjectStore.class);

  /** The following API
   *
   *  - executeJDOQLSelect
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public Collection<?> executeJDOQLSelect(String queryStr) throws Exception {
    boolean committed = false;
    Collection<?> result = null;
    try {
      openTransaction();
      try (Query query = pm.newQuery(queryStr)) {
        result = Collections.unmodifiableCollection(new ArrayList<>(((Collection<?>) query.execute())));
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        result = null;
        rollbackTransaction();
      }
    }
    return result;
  }

  /** The following API
   *
   *  - executeJDOQLUpdate
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public long executeJDOQLUpdate(String queryStr) throws Exception {
    boolean committed = false;
    long numUpdated = 0L;
    try {
      openTransaction();
      try (Query query = pm.newQuery(queryStr)) {
        numUpdated = (Long) query.execute();
      }
      committed = commitTransaction();
      if (committed) {
        return numUpdated;
      }
    } finally {
      rollbackAndCleanup(committed, null);
    }
    return -1L;
  }

  /** The following API
   *
   *  - listFSRoots
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public Set<String> listFSRoots() {
    boolean committed = false;
    Query query = null;
    Set<String> fsRoots = new HashSet<>();
    try {
      openTransaction();
      query = pm.newQuery(MDatabase.class);
      List<MDatabase> mDBs = (List<MDatabase>) query.execute();
      pm.retrieveAll(mDBs);
      for (MDatabase mDB : mDBs) {
        fsRoots.add(mDB.getLocationUri());
      }
      committed = commitTransaction();
      if (committed) {
        return fsRoots;
      } else {
        return null;
      }
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  private boolean shouldUpdateURI(URI onDiskUri, URI inputUri) {
    String onDiskHost = onDiskUri.getHost();
    String inputHost = inputUri.getHost();

    int onDiskPort = onDiskUri.getPort();
    int inputPort = inputUri.getPort();

    String onDiskScheme = onDiskUri.getScheme();
    String inputScheme = inputUri.getScheme();

    //compare ports
    if (inputPort != -1) {
      if (inputPort != onDiskPort) {
        return false;
      }
    }
    //compare schemes
    if (inputScheme != null) {
      if (onDiskScheme == null) {
        return false;
      }
      if (!inputScheme.equalsIgnoreCase(onDiskScheme)) {
        return false;
      }
    }
    //compare hosts
    if (onDiskHost != null) {
      return inputHost.equalsIgnoreCase(onDiskHost);
    } else {
      return false;
    }
  }

  public class UpdateMDatabaseURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateMDatabaseURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
   *
   *  - updateMDatabaseURI
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public UpdateMDatabaseURIRetVal updateMDatabaseURI(URI oldLoc, URI newLoc, boolean dryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
    UpdateMDatabaseURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MDatabase.class);
      List<MDatabase> mDBs = (List<MDatabase>) query.execute();
      pm.retrieveAll(mDBs);

      for (MDatabase mDB : mDBs) {
        URI locationURI = null;
        String location = mDB.getLocationUri();
        try {
          locationURI = new Path(location).toUri();
        } catch (IllegalArgumentException e) {
          badRecords.add(location);
        }
        if (locationURI == null) {
          badRecords.add(location);
        } else {
          if (shouldUpdateURI(locationURI, oldLoc)) {
            String dbLoc = mDB.getLocationUri().replaceAll(oldLoc.toString(), newLoc.toString());
            updateLocations.put(locationURI.toString(), dbLoc);
            if (!dryRun) {
              mDB.setLocationUri(dbLoc);
            }
          }
        }

        // if managed location is set, perform location update for managed location URI as well
        if (org.apache.commons.lang3.StringUtils.isNotBlank(mDB.getManagedLocationUri())) {
          URI managedLocationURI = null;
          String managedLocation = mDB.getManagedLocationUri();
          try {
            managedLocationURI = new Path(managedLocation).toUri();
          } catch (IllegalArgumentException e) {
            badRecords.add(managedLocation);
          }
          if (managedLocationURI == null) {
            badRecords.add(managedLocation);
          } else {
            if (shouldUpdateURI(managedLocationURI, oldLoc)) {
              String dbLoc = mDB.getManagedLocationUri().replaceAll(oldLoc.toString(), newLoc.toString());
              updateLocations.put(managedLocationURI.toString(), dbLoc);
              if (!dryRun) {
                mDB.setManagedLocationUri(dbLoc);
              }
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMDatabaseURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  public class UpdatePropURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdatePropURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  private void updatePropURIHelper(URI oldLoc, URI newLoc, String tblPropKey, boolean isDryRun,
      List<String> badRecords, Map<String, String> updateLocations,
      Map<String, String> parameters) {
    URI tablePropLocationURI = null;
    if (parameters.containsKey(tblPropKey)) {
      String tablePropLocation = parameters.get(tblPropKey);
      try {
        tablePropLocationURI = new Path(tablePropLocation).toUri();
      } catch (IllegalArgumentException e) {
        badRecords.add(tablePropLocation);
      }
      // if tablePropKey that was passed in lead to a valid URI resolution, update it if
      //parts of it match the old-NN-loc, else add to badRecords
      if (tablePropLocationURI == null) {
        badRecords.add(tablePropLocation);
      } else {
        if (shouldUpdateURI(tablePropLocationURI, oldLoc)) {
          String tblPropLoc = parameters.get(tblPropKey).replaceAll(oldLoc.toString(), newLoc
              .toString());
          updateLocations.put(tablePropLocationURI.toString(), tblPropLoc);
          if (!isDryRun) {
            parameters.put(tblPropKey, tblPropLoc);
          }
        }
      }
    }
  }

  /** The following APIs
   *
   *  - updateMStorageDescriptorTblPropURI
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public UpdatePropURIRetVal updateTblPropURI(URI oldLoc, URI newLoc, String tblPropKey,
      boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
    UpdatePropURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MTable.class);
      List<MTable> mTbls = (List<MTable>) query.execute();
      pm.retrieveAll(mTbls);

      for (MTable mTbl : mTbls) {
        updatePropURIHelper(oldLoc, newLoc, tblPropKey, isDryRun, badRecords, updateLocations,
            mTbl.getParameters());
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdatePropURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  /** The following APIs
   *
   *  - updateMStorageDescriptorTblPropURI
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  @Deprecated
  public UpdatePropURIRetVal updateMStorageDescriptorTblPropURI(URI oldLoc, URI newLoc,
      String tblPropKey, boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
    UpdatePropURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MStorageDescriptor.class);
      List<MStorageDescriptor> mSDSs = (List<MStorageDescriptor>) query.execute();
      pm.retrieveAll(mSDSs);
      for (MStorageDescriptor mSDS : mSDSs) {
        updatePropURIHelper(oldLoc, newLoc, tblPropKey, isDryRun, badRecords, updateLocations,
            mSDS.getParameters());
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdatePropURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  public class UpdateMStorageDescriptorTblURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;
    private int numNullRecords;

    UpdateMStorageDescriptorTblURIRetVal(List<String> badRecords,
        Map<String, String> updateLocations, int numNullRecords) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
      this.numNullRecords = numNullRecords;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }

    public int getNumNullRecords() {
      return numNullRecords;
    }

    public void setNumNullRecords(int numNullRecords) {
      this.numNullRecords = numNullRecords;
    }
  }

  /** The following APIs
   *
   *  - updateMStorageDescriptorTblURI
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public UpdateMStorageDescriptorTblURIRetVal updateMStorageDescriptorTblURI(URI oldLoc,
      URI newLoc, boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
    int numNullRecords = 0;
    UpdateMStorageDescriptorTblURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MStorageDescriptor.class);
      List<MStorageDescriptor> mSDSs = (List<MStorageDescriptor>) query.execute();
      pm.retrieveAll(mSDSs);
      for (MStorageDescriptor mSDS : mSDSs) {
        URI locationURI = null;
        String location = mSDS.getLocation();
        if (location == null) { // This can happen for View or Index
          numNullRecords++;
          continue;
        }
        try {
          locationURI = new Path(location).toUri();
        } catch (IllegalArgumentException e) {
          badRecords.add(location);
        }
        if (locationURI == null) {
          badRecords.add(location);
        } else {
          if (shouldUpdateURI(locationURI, oldLoc)) {
            String tblLoc = mSDS.getLocation().replaceAll(oldLoc.toString(), newLoc.toString());
            updateLocations.put(locationURI.toString(), tblLoc);
            if (!isDryRun) {
              mSDS.setLocation(tblLoc);
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMStorageDescriptorTblURIRetVal(badRecords, updateLocations, numNullRecords);
      }
      return retVal;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  public class UpdateSerdeURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateSerdeURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
   *
   *  - updateSerdeURI
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public UpdateSerdeURIRetVal updateSerdeURI(URI oldLoc, URI newLoc, String serdeProp,
      boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
    UpdateSerdeURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MSerDeInfo.class);
      List<MSerDeInfo> mSerdes = (List<MSerDeInfo>) query.execute();
      pm.retrieveAll(mSerdes);
      for (MSerDeInfo mSerde : mSerdes) {
        if (mSerde.getParameters().containsKey(serdeProp)) {
          String schemaLoc = mSerde.getParameters().get(serdeProp);
          URI schemaLocURI = null;
          try {
            schemaLocURI = new Path(schemaLoc).toUri();
          } catch (IllegalArgumentException e) {
            badRecords.add(schemaLoc);
          }
          if (schemaLocURI == null) {
            badRecords.add(schemaLoc);
          } else {
            if (shouldUpdateURI(schemaLocURI, oldLoc)) {
              String newSchemaLoc = schemaLoc.replaceAll(oldLoc.toString(), newLoc.toString());
              updateLocations.put(schemaLocURI.toString(), newSchemaLoc);
              if (!isDryRun) {
                mSerde.getParameters().put(serdeProp, newSchemaLoc);
              }
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateSerdeURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  /**
   * Using resultSet to read the HMS_SUMMARY table.
   * @param catalogFilter the optional catalog name filter
   * @param dbFilter the optional database name filter
   * @param tableFilter the optional table name filter
   * @return MetadataSummary
   * @throws MetaException
   */
  public List<MetadataTableSummary> getMetadataSummary(String catalogFilter, String dbFilter, String tableFilter)
      throws MetaException {
    JDOConnection jdoConn = null;
    Map<Long, MetadataTableSummary> partedTabs = new HashMap<>();
    Map<Long, MetadataTableSummary> nonPartedTabs = new HashMap<>();
    List<MetadataTableSummary> metadataTableSummaryList = new ArrayList<>();
    final String query = sqlGenerator.getSelectQueryForMetastoreSummary();
    try {
      jdoConn = pm.getDataStoreConnection();
      if (!prepareGetMetaSummary(jdoConn)) {
        return metadataTableSummaryList;
      }
      try (Statement stmt = ((Connection) jdoConn.getNativeConnection()).createStatement()) {
        stmt.setFetchSize(0);
        try (ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            MetadataTableSummary summary = new MetadataTableSummary(rs.getString("CTLG"),
                rs.getString("NAME"), rs.getString("TBL_NAME"));
            feedColumnSummary(summary, rs);
            feedTabFormatSummary(summary, rs);
            metadataTableSummaryList.add(summary);
            long tableId = rs.getLong("TBL_ID");
            if (summary.getPartitionCount() > 0) {
              partedTabs.put(tableId, summary);
            } else if (summary.getPartitionColumnCount() == 0) {
              nonPartedTabs.put(tableId, summary);
            }
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("Exception while running the query " + query, e);
      throw new MetaException("Exception while computing the summary: " + e.getMessage());
    } finally {
      if (jdoConn != null) {
        jdoConn.close();
      }
    }
    feedBasicStats(nonPartedTabs, partedTabs);
    return metadataTableSummaryList;
  }

  private void feedColumnSummary(MetadataTableSummary summary, ResultSet rs) throws SQLException {
    String partitionColumn = rs.getString("PARTITION_COLUMN");
    int partitionColumnCount = (partitionColumn == null) ? 0 : (partitionColumn.split(",")).length;
    summary
        .partitionSummary(partitionColumnCount, rs.getInt("PARTITION_CNT"))
        .columnSummary(
            rs.getInt("TOTAL_COLUMN_COUNT"),
            rs.getInt("ARRAY_COLUMN_COUNT"),
            rs.getInt("STRUCT_COLUMN_COUNT"),
            rs.getInt("MAP_COLUMN_COUNT"));
  }

  private void feedTabFormatSummary(MetadataTableSummary summary, ResultSet rs) throws SQLException {
    String tblType = rs.getString("TBL_TYPE");
    String fileType = extractFileFormat(rs.getString("SLIB"));
    Set<String> nonNativeTabTypes = new HashSet<>(Arrays.asList("jdbc", "kudu", "hbase", "iceberg"));
    if (nonNativeTabTypes.contains(fileType)) {
      tblType = fileType.toUpperCase();
      if ("iceberg".equalsIgnoreCase(tblType)) {
        String writeFormatDefault = rs.getString("WRITE_FORMAT_DEFAULT");
        if (writeFormatDefault == null) {
          writeFormatDefault = "null";
        }
        if (writeFormatDefault.equals("null")) {
          fileType = "parquet";
        } else {
          fileType = writeFormatDefault;
        }
      }
    } else if (TableType.MANAGED_TABLE.name().equalsIgnoreCase(tblType)) {
      String transactionalProperties = rs.getString("TRANSACTIONAL_PROPERTIES");
      if (transactionalProperties == null) {
        transactionalProperties = "null";
      }
      tblType = "insert_only".equalsIgnoreCase(transactionalProperties.trim()) ?
          "HIVE_ACID_INSERT_ONLY" : "HIVE_ACID_FULL";
    } else if (TableType.EXTERNAL_TABLE.name().equalsIgnoreCase(tblType)) {
      tblType = "HIVE_EXTERNAL";
    } else {
      tblType = tblType != null ? tblType.toUpperCase() : "NULL";
    }
    String compressionType = rs.getString("IS_COMPRESSED");
    if (compressionType.equals("0") || compressionType.equals("f")) {
      compressionType = "None";
    }
    summary.tableFormatSummary(tblType, compressionType, fileType);
  }

  private boolean prepareGetMetaSummary(JDOConnection jdoConn) throws MetaException {
    try {
      List<String> prepareQueries = sqlGenerator.getCreateQueriesForMetastoreSummary();
      if (prepareQueries == null || prepareQueries.isEmpty()) {
        LOG.warn("Metadata summary has not been implemented for dbtype {}", DatabaseProduct.dbType);
        return false;
      }
      try (Statement stmt = ((Connection) jdoConn.getNativeConnection()).createStatement()) {
        long startTime = System.currentTimeMillis();
        for (String q : prepareQueries) {
          stmt.addBatch(q);
        }
        stmt.executeBatch();
        long endTime = System.currentTimeMillis();
        LOG.info("Total query time for generating HMS Summary: {} (ms)", endTime - startTime);
      }
    } catch (SQLException e) {
      LOG.error("Exception during computing HMS Summary", e);
      throw new MetaException("Error preparing the context for computing the summary: " + e.getMessage());
    }
    return true;
  }

  private void feedBasicStats(final Map<Long, MetadataTableSummary> nonPartedTabs,
      final Map<Long, MetadataTableSummary> partedTabs) throws MetaException {
    runBatched(batchSize, new ArrayList<>(nonPartedTabs.keySet()), new Batchable<Long, Void>() {
      static final String QUERY_TEXT0 = "select \"TBL_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from \"TABLE_PARAMS\" where \"PARAM_KEY\" " +
          "in ('" + StatsSetupConst.TOTAL_SIZE + "', '" + StatsSetupConst.NUM_FILES + "', '" + StatsSetupConst.ROW_COUNT + "') and \"TBL_ID\" in (";
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        feedBasicStats(QUERY_TEXT0, input, nonPartedTabs, "");
        return Collections.emptyList();
      }
    });

   runBatched(batchSize, new ArrayList<>(partedTabs.keySet()), new Batchable<Long, Void>() {
     static final String QUERY_TEXT0 = "select \"TBL_ID\", \"PARAM_KEY\", sum(CAST(\"PARAM_VALUE\" AS decimal(21,0))) from \"PARTITIONS\" t " +
         "join \"PARTITION_PARAMS\" p on p.\"PART_ID\" = t.\"PART_ID\" where \"PARAM_KEY\" " +
          "in ('" + StatsSetupConst.TOTAL_SIZE + "', '" + StatsSetupConst.NUM_FILES + "', '" + StatsSetupConst.ROW_COUNT + "') and t.\"TBL_ID\" in (";
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        feedBasicStats(QUERY_TEXT0, input, partedTabs, " group by \"TBL_ID\", \"PARAM_KEY\"");
        return Collections.emptyList();
      }
    });
  }

  private void feedBasicStats(String queryText0, List<Long> input, Map<Long, MetadataTableSummary> summaryMap, String subQ) {
    int size = input.size();
    String queryText = queryText0 + (size == 0 ? "" : repeat(",?", size).substring(1)) + ")" + subQ;
    if (dbType.isMYSQL()) {
      queryText = queryText.replace("\"", "");
    }
    Object[] params = new Object[size];
    for (int i = 0; i < input.size(); ++i) {
      params[i] = input.get(i);
    }
    Query<?> query = null;
    try {
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      List<Object[]> result = (List<Object[]>) query.executeWithArray(params);
      if (result != null) {
        for (Object[] fields : result) {
          Long tabId = Long.parseLong(String.valueOf(fields[0]));
          MetadataTableSummary summary = summaryMap.get(tabId);
          feedBasicStats(summary, String.valueOf(fields[1]), fields[2]);
        }
      }
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private void feedBasicStats(MetadataTableSummary summary, String key, Object value) {
    if (summary == null || value == null ||
        !org.apache.commons.lang3.StringUtils.isNumeric(value.toString())) {
      return;
    }
    long val = Long.parseLong(value.toString());
    switch (key) {
    case StatsSetupConst.TOTAL_SIZE:
      summary.setTotalSize(summary.getTotalSize() + val);
      break;
    case StatsSetupConst.ROW_COUNT:
      summary.setNumRows(summary.getNumRows() + val);
      break;
    case StatsSetupConst.NUM_FILES:
      summary.setNumFiles(summary.getNumFiles() + val);
      break;
    default:
      throw new AssertionError("This should never happen!");
    }
  }

  /**
   * Helper method for getMetadataSummary. Extracting the format of the file from the long string.
   * @param fileFormat - fileFormat. A long String which indicates the type of the file.
   * @return String A short String which indicates the type of the file.
   */
  private static String extractFileFormat(String fileFormat) {
    if (fileFormat == null) {
      return "NULL";
    }
    final String lowerCaseFileFormat = fileFormat.toLowerCase();
    Set<String> fileTypes = new HashSet<>(Arrays.asList("iceberg", "parquet", "orc", "avro", "json", "hbase", "jdbc",
        "kudu", "text", "sequence", "opencsv", "lazysimple", "passthrough"));
    Optional<String> result = fileTypes.stream().filter(lowerCaseFileFormat::contains).findFirst();
    if (result.isPresent()) {
      String file = result.get();
      if ("opencsv".equals(file)) {
        return "openCSV";
      } else if ("lazysimple".equals(file)) {
        return "text";
      }
      return file;
    }
    return fileFormat;
  }

  public Set<TableName> filterTablesForSummary(List<Pair<TableName, MetadataTableSummary>> tableSummaries,
      Integer lastUpdatedDays, Integer tablesLimit) {
    if (tableSummaries == null || tableSummaries.isEmpty()) {
      return Collections.emptySet();
    }
    Set<TableName> tableNames = tableSummaries.stream().map(Pair::getLeft).collect(Collectors.toSet());
    if (lastUpdatedDays == null && (tablesLimit == null || tableNames.size() < tablesLimit)) {
      return tableNames;
    }
    String tableType = tableSummaries.get(0).getRight().getTableType();
    if (!"iceberg".equalsIgnoreCase(tableType)) {
      // we don't support filtering this type yet, ignore...
      LOG.warn("This table type: {} hasn't been supported selecting the summary yet, ignore...", tableType);
      return tableNames;
    }

    Set<String> dbs = new HashSet<>();
    tableSummaries.forEach(ts -> dbs.add(normalizeIdentifier(ts.getLeft().getDb())));
    boolean success = false;
    Query<?> query = null;
    try {
      String catalog = normalizeIdentifier(tableSummaries.get(0).getLeft().getCat());
      if (StringUtils.isEmpty(catalog)) {
        catalog = MetaStoreUtils.getDefaultCatalog(conf);
      }
      openTransaction();
      String filter = "database.catalogName == t1 && t2.contains(database.name) && " +
          "parameters.get(\"table_type\") == t3 && parameters.get(\"current-snapshot-timestamp-ms\") > t4";
      query = pm.newQuery(MTable.class, filter) ;
      query.declareParameters("java.lang.String t1, java.util.Collection t2, java.lang.String t3, java.lang.Long t4");
      query.setResult("database.name, tableName");
      query.setOrdering("parameters.get(\"current-snapshot-timestamp-ms\") DESC");
      if (tablesLimit != null && tablesLimit >= 0) {
        query.setRange(0, tablesLimit);
      }
      assert lastUpdatedDays != null;
      List<Object[]> result = (List<Object[]>) query.executeWithArray(catalog, dbs, tableType.toUpperCase(),
          System.currentTimeMillis() - lastUpdatedDays * 24 * 3600000L);
      if (result == null || result.isEmpty()) {
        return Collections.emptySet();
      }
      Set<TableName> qr = new HashSet<>();
      for (Object[] fields : result) {
        qr.add(new TableName(catalog, (String)fields[0], (String) fields[1]));
      }
      tableNames = tableNames.stream().filter(qr::contains).collect(Collectors.toSet());
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, query);
    }
    return tableNames;
  }
}
