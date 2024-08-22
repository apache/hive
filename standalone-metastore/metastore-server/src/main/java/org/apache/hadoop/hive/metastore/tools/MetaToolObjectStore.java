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
import java.math.BigInteger;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.tools.metatool.IcebergTableMetadataHandler;
import org.apache.hadoop.hive.metastore.tools.metatool.MetadataTableSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /** The following APIs
   *
   *  - getMetadataSummary
   *
   * is used by HiveMetaTool.
   */

  /**
   * Using resultSet to read the HMS_SUMMARY table.
   * @param catalogFilter the optional catalog name filter
   * @param dbFilter the optional database name filter
   * @param tableFilter the optional table name filter
   * @return MetadataSummary
   * @throws SQLException
   */
  public List<MetadataTableSummary> getMetadataSummary(String catalogFilter, String dbFilter, String tableFilter) throws SQLException {
    ArrayList<MetadataTableSummary> metadataTableSummaryList = new ArrayList<MetadataTableSummary>();

    ResultSet rs = null;
    Statement stmt = null;
    // Fetch the metrics from iceberg manifest for iceberg tables in hive
    IcebergTableMetadataHandler icebergHandler = null;
    Map<String, MetadataTableSummary> icebergTableSummaryMap = null;

    try {
      icebergHandler = new IcebergTableMetadataHandler(conf);
      icebergTableSummaryMap = icebergHandler.getIcebergTables();
    } catch (Exception e) {
      LOG.error("Unable to fetch metadata from iceberg manifests", e);
    }

    List<String> querySet = sqlGenerator.getCreateQueriesForMetastoreSummary();
    if (querySet == null) {
      LOG.warn("Metadata summary has not been implemented for dbtype {}", sqlGenerator.getDbProduct().dbType);
      return null;
    }

    try {
      JDOConnection jdoConn = null;
      jdoConn = pm.getDataStoreConnection();
      stmt = ((Connection) jdoConn.getNativeConnection()).createStatement();
      long startTime = System.currentTimeMillis();

      for (String q: querySet) {
        stmt.execute(q);
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Total query time for generating HMS Summary: {} (ms)", (((long)endTime) - ((long)startTime)));
    } catch (SQLException e) {
      LOG.error("Exception during computing HMS Summary", e);
      throw e;
    }

    final String query = sqlGenerator.getSelectQueryForMetastoreSummary();
    try {
      String tblName, dbName, ctlgName, tblType, fileType, compressionType, partitionColumn, writeFormatDefault, transactionalProperties;
      int colCount, arrayColCount, structColCount, mapColCount;
      Integer partitionCnt;
      BigInteger totalSize, sizeNumRows, sizeNumFiles;
      stmt.setFetchSize(0);
      rs = stmt.executeQuery(query);
      while (rs.next()) {
        tblName = rs.getString("TBL_NAME");
        dbName = rs.getString("NAME");
        ctlgName = rs.getString("CTLG");
        if(ctlgName == null) ctlgName = "null";
        colCount = rs.getInt("TOTAL_COLUMN_COUNT");
        arrayColCount = rs.getInt("ARRAY_COLUMN_COUNT");
        structColCount = rs.getInt("STRUCT_COLUMN_COUNT");
        mapColCount = rs.getInt("MAP_COLUMN_COUNT");
        tblType = rs.getString("TBL_TYPE");
        fileType = rs.getString("SLIB");
        if (fileType != null) fileType = extractFileFormat(fileType);
        compressionType = rs.getString("IS_COMPRESSED");
        if (compressionType.equals("0") || compressionType.equals("f")) compressionType = "None";
        partitionColumn = rs.getString("PARTITION_COLUMN");
        int partitionColumnCount = (partitionColumn == null) ? 0 : (partitionColumn.split(",")).length;
        partitionCnt = rs.getInt("PARTITION_CNT");

        totalSize = BigInteger.valueOf(rs.getLong("TOTAL_SIZE"));
        sizeNumRows = BigInteger.valueOf(rs.getLong("NUM_ROWS"));
        sizeNumFiles = BigInteger.valueOf(rs.getLong("NUM_FILES"));

        writeFormatDefault = rs.getString("WRITE_FORMAT_DEFAULT");
        if (writeFormatDefault == null) writeFormatDefault = "null";
        transactionalProperties = rs.getString("TRANSACTIONAL_PROPERTIES");
        if (transactionalProperties == null) transactionalProperties = "null";

        // for iceberg tables, overwrite the metadata by the metadata fetched in HMSSummaryIcebergHandler
        if (fileType != null && fileType.equals("iceberg") && icebergHandler.isEnabled() && icebergTableSummaryMap != null) {
          // if the new metadata is not null or 0, overwrite the old metadata
          MetadataTableSummary icebergTableSummary = icebergTableSummaryMap.get(tblName);
          ctlgName = icebergTableSummary.getCtlgName() != null ? icebergTableSummary.getCtlgName() : ctlgName;
          dbName = icebergTableSummary.getDbName() != null ? icebergTableSummary.getDbName() : dbName;
          colCount = icebergTableSummary.getColCount() != 0 ? icebergTableSummary.getColCount() : colCount;
          partitionColumnCount = icebergTableSummary.getPartitionColumnCount() != 0 ? icebergTableSummary.getPartitionColumnCount() : partitionColumnCount;
          totalSize = icebergTableSummary.getTotalSize() != null ? icebergTableSummary.getTotalSize() : totalSize;
          sizeNumRows = icebergTableSummary.getSizeNumRows() != null ? icebergTableSummary.getSizeNumRows(): sizeNumRows;
          sizeNumFiles = icebergTableSummary.getSizeNumFiles() != null ? icebergTableSummary.getSizeNumFiles(): sizeNumFiles;
          if (writeFormatDefault.equals("null")){
            fileType = "parquet";
          }
          else {
            fileType = writeFormatDefault;
          }
          tblType = "ICEBERG";
        }

        if (tblType.equals("EXTERNAL_TABLE")){
          if (fileType.equals("parquet")){
            tblType = "HIVE_EXTERNAL";
          }
          else if (fileType.equals("jdbc")){
            tblType = "JDBC";
          }
          else if (fileType.equals("kudu")){
            tblType = "KUDU";
          }
          else if (fileType.equals("hbase")){
            tblType = "HBASE";
          } else {
            tblType = "HIVE_EXTERNAL";
          }
        }

        if (tblType.equals("MANAGED_TABLE")){
          if (transactionalProperties == "insert_only"){
            tblType = "HIVE_ACID_INSERT_ONLY";
          }
          else {
            tblType = "HIVE_ACID_FULL";
          }
        }

        MetadataTableSummary summary = new MetadataTableSummary(ctlgName, dbName, tblName, colCount,
            partitionColumnCount, partitionCnt, totalSize, sizeNumRows, sizeNumFiles, tblType,
            fileType, compressionType, arrayColCount, structColCount, mapColCount);
        metadataTableSummaryList.add(summary);
      }
    } catch (Exception e) {
      String msg = "Runtime exception while running the query " + query;
      LOG.error(msg, e);
      throw e;
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
    }
    return metadataTableSummaryList;
  }

  /**
   * Helper method for getMetadataSummary. Extracting the format of the file from the long string.
   * @param fileFormat - fileFormat. A long String which indicates the type of the file.
   * @return String A short String which indicates the type of the file.
   */
  private static String extractFileFormat(String fileFormat) {
    String lowerCaseFileFormat = null;
    if (fileFormat == null) {
      return "NULL";
    }
    lowerCaseFileFormat = fileFormat.toLowerCase();
    if (lowerCaseFileFormat.contains("iceberg")) {
      fileFormat = "iceberg";
    } else if (lowerCaseFileFormat.contains("parquet")) {
      fileFormat = "parquet";
    } else if (lowerCaseFileFormat.contains("orc")) {
      fileFormat = "orc";
    } else if (lowerCaseFileFormat.contains("avro")) {
      fileFormat = "avro";
    } else if (lowerCaseFileFormat.contains("json")) {
      fileFormat = "json";
    } else if (lowerCaseFileFormat.contains("hbase")) {
      fileFormat = "hbase";
    } else if (lowerCaseFileFormat.contains("jdbc")) {
      fileFormat = "jdbc";
    } else if (lowerCaseFileFormat.contains("kudu")) {
      fileFormat = "kudu";
    } else if ((lowerCaseFileFormat.contains("text")) || (lowerCaseFileFormat.contains("lazysimple"))) {
      fileFormat = "text";
    } else if (lowerCaseFileFormat.contains("sequence")) {
      fileFormat = "sequence";
    } else if (lowerCaseFileFormat.contains("passthrough")) {
      fileFormat = "passthrough";
    } else if (lowerCaseFileFormat.contains("opencsv")) {
      fileFormat = "openCSV";
    }
    return fileFormat;
  }

}
