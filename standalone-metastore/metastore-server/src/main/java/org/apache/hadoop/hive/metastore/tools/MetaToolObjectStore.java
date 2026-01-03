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
import java.lang.reflect.Modifier;
import java.net.URI;
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
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.repeat;

/**
 * This class should be used in metatool only
 */
public class MetaToolObjectStore extends ObjectStore {

  private static final Logger LOG = LoggerFactory.getLogger(MetaToolObjectStore.class);

  public static class TableFormat {
    private TableFormat() {
      // private constructor
    }
    public static final String PARQUET = "parquet";
    public static final String ORC = "orc";
    public static final String AVRO = "avro";
    public static final String JSON = "json";
    public static final String HBASE = "hbase";
    public static final String JDBC = "jdbc";
    public static final String KUDU = "kudu";
    public static final String ICEBERG = "iceberg";
    public static final String TEXT = "text";
    public static final String SEQUENCE = "sequence";
    public static final String OPENCSV = "opencsv";
    public static final String LAZY_SIMPLE = "lazysimple";
    public static final String PASS_THROUGH = "passthrough";

    private static final Set<String> AVAILABLE_FORMATS = new HashSet<>();
    static {
      Arrays.stream(TableFormat.class.getDeclaredFields())
          .filter(field -> Modifier.isStatic(field.getModifiers()) && Modifier.isPublic(field.getModifiers()))
          .map(field -> {
            try {
              return field.get(null);
            } catch (IllegalAccessException e) {
              throw new AssertionError("This should not happen");
            }
          })
          .filter(String.class::isInstance)
          .forEach(res -> AVAILABLE_FORMATS.add((String) res));
    }

    public static Set<String> getNonNativeFormats() {
      return new HashSet<>(Arrays.asList(HBASE, JDBC, KUDU, ICEBERG));
    }

    /**
     * Helper method for getMetadataSummary. Extracting the format of the file from the long string.
     * @param fileFormat - fileFormat. A long String which indicates the type of the file.
     * @return String A short String which indicates the type of the file.
     */
    public static String extractFileFormat(String fileFormat) {
      if (fileFormat == null) {
        return "NULL";
      }
      final String lowerCaseFileFormat = fileFormat.toLowerCase();
      Optional<String> result = AVAILABLE_FORMATS.stream().filter(lowerCaseFileFormat::contains).findFirst();
      if (result.isPresent()) {
        String file = result.get();
        if (OPENCSV.equals(file)) {
          return "openCSV";
        } else if (LAZY_SIMPLE.equals(file)) {
          return TEXT;
        }
        return file;
      }
      return fileFormat;
    }

    public static boolean isIcebergFormat(String tableFormat) {
      return ICEBERG.equalsIgnoreCase(tableFormat);
    }
  }


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
  public List<MetadataTableSummary> getMetadataSummary(String catalogFilter,
      String dbFilter, String tableFilter) throws MetaException {
    Set<Long> partedTabs = new HashSet<>();
    Set<Long> nonPartedTabs = new HashSet<>();
    Map<Long, MetadataTableSummary> summaries = new HashMap<>();
    List<MetadataTableSummary> metadataTableSummaryList = new ArrayList<>();
    StringBuilder filter = new StringBuilder();
    List<String> parameterVals = new ArrayList<>();
    if (!StringUtils.isEmpty(dbFilter)) {
      appendPatternCondition(filter, "database.name", dbFilter, parameterVals);
    }
    if (!StringUtils.isEmpty(tableFilter)) {
      appendPatternCondition(filter, "tableName", tableFilter, parameterVals);
    }
    try (QueryWrapper query = new QueryWrapper(filter.length() > 0 ?
        pm.newQuery(MTable.class, filter.toString()) : pm.newQuery(MTable.class))){
      query.setResult("id, database.catalogName, database.name, tableName, owner, tableType");
      List<Object[]> tables = (List<Object[]>) query.executeWithArray(parameterVals.toArray(new String[0]));
      for (Object[] table : tables) {
        Deadline.checkTimeout();
        long tableId = Long.parseLong(table[0].toString());
        MetadataTableSummary summary = new MetadataTableSummary(String.valueOf(table[1]),
            String.valueOf(table[2]), String.valueOf(table[3]), String.valueOf(table[4]));
        summary.setTableType(String.valueOf(table[5]));
        summary.setTableId(tableId);
        summaries.put(tableId, summary);
        metadataTableSummaryList.add(summary);
      }
    }
    collectColumnSummary(summaries);
    collectTabFormatSummary(summaries);
    collectPartitionSummary(summaries, partedTabs, nonPartedTabs);
    collectBasicStats(summaries, nonPartedTabs, partedTabs);
    return metadataTableSummaryList;
  }

  private void collectPartitionSummary(Map<Long, MetadataTableSummary> summaries,  Set<Long> partedTabs,
      Set<Long> nonPartedTabs) throws MetaException {
    String queryText0 = "select \"TBL_ID\", count(1) from \"PARTITION_KEYS\" where \"TBL_ID\" in (";
    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        Pair<Query<?>, List<Object[]>> qResult = getResultFromInput(input, queryText0, " group by \"TBL_ID\"");
        try {
          List<Object[]> result = qResult.getRight();
          if (result != null) {
            for (Object[] fields : result) {
              Deadline.checkTimeout();
              Long tabId = Long.parseLong(String.valueOf(fields[0]));
              MetadataTableSummary summary = summaries.get(tabId);
              summary.setPartitionColumnCount(Integer.parseInt(fields[1].toString()));
              partedTabs.add(tabId);
            }
          }
          summaries.keySet().stream().filter(k -> !partedTabs.contains(k))
              .forEach(nonPartedTabs::add);
        } finally {
          qResult.getLeft().closeAll();
        }
        return Collections.emptyList();
      }
    }.runBatched(batchSize, new ArrayList<>(summaries.keySet()));
    String queryText1 = "select \"TBL_ID\", count(1) from \"PARTITIONS\" where \"TBL_ID\" in (";
    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        Pair<Query<?>, List<Object[]>> qResult = getResultFromInput(input, queryText1, " group by \"TBL_ID\"");
        try {
          List<Object[]> result = qResult.getRight();
          if (result != null) {
            for (Object[] fields : result) {
              Deadline.checkTimeout();
              Long tabId = Long.parseLong(String.valueOf(fields[0]));
              MetadataTableSummary summary = summaries.get(tabId);
              summary.setPartitionCount(Integer.parseInt(fields[1].toString()));
            }
          }
        } finally {
          qResult.getLeft().closeAll();
        }
        return Collections.emptyList();
      }
    }.runBatched(batchSize, new ArrayList<>(partedTabs));
  }

  private void collectColumnSummary(Map<Long, MetadataTableSummary> summaries) throws MetaException {
    String queryText0 = "select \"TBL_ID\", count(*), sum(CASE WHEN \"TYPE_NAME\" like 'array%' THEN 1 ELSE 0 END)," +
        " sum(CASE WHEN \"TYPE_NAME\" like 'struct%' THEN 1 ELSE 0 END), sum(CASE WHEN \"TYPE_NAME\" like 'map%' THEN 1 ELSE 0 END)" +
        " from \"TBLS\" t join \"SDS\" s on t.\"SD_ID\" = s.\"SD_ID\" join \"CDS\" c on s.\"CD_ID\" = c.\"CD_ID\" join \"COLUMNS_V2\" v on c.\"CD_ID\" = v.\"CD_ID\"" +
        " where \"TBL_ID\" in (";
    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        Pair<Query<?>, List<Object[]>> qResult = getResultFromInput(input, queryText0, " group by \"TBL_ID\"");
        try {
          List<Object[]> result = qResult.getRight();
          if (result != null) {
            for (Object[] fields : result) {
              Deadline.checkTimeout();
              Long tabId = Long.parseLong(String.valueOf(fields[0]));
              MetadataTableSummary summary = summaries.get(tabId);
              summary.columnSummary(Integer.parseInt(fields[1].toString()), Integer.parseInt(fields[2].toString()),
                  Integer.parseInt(fields[3].toString()), Integer.parseInt(fields[4].toString()));
            }
          }
        } finally {
          qResult.getLeft().closeAll();
        }
        return Collections.emptyList();
      }
    }.runBatched(batchSize, new ArrayList<>(summaries.keySet()));
  }

  private void collectTabFormatSummary(Map<Long, MetadataTableSummary> summaries) throws MetaException {
    String queryText0 = "select t.\"TBL_ID\", d.\"SLIB\", s.\"IS_COMPRESSED\" from \"TBLS\" t left join \"SDS\" s on t.\"SD_ID\" = s.\"SD_ID\" left join \"SERDES\" d on d.\"SERDE_ID\" = s.\"SERDE_ID\"" +
        " where t.\"TBL_ID\" in (";
    String queryText1 = "select p.\"TBL_ID\", " + dbType.toVarChar("p.\"PARAM_VALUE\"") + " from \"TABLE_PARAMS\" p " +
        " where p.\"PARAM_KEY\" = 'transactional_properties' and p.\"TBL_ID\" in (";
    List<Long> transactionTables = new ArrayList<>();
    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        Pair<Query<?>, List<Object[]>> qResult = getResultFromInput(input, queryText0, "");
        try {
          List<Object[]> result = qResult.getRight();
          if (result != null) {
            for (Object[] fields : result) {
              Deadline.checkTimeout();
              Long tabId = Long.parseLong(String.valueOf(fields[0]));
              MetadataTableSummary summary = summaries.get(tabId);
              String lib = String.valueOf(fields[1]);
              String compressionType = String.valueOf(fields[2]);
              collectTabFormatSummary(transactionTables, tabId, summary, lib, compressionType);
            }
          }
        } finally {
          qResult.getLeft().closeAll();
        }
        return Collections.emptyList();
      }
    }.runBatched(batchSize, new ArrayList<>(summaries.keySet()));

    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        Pair<Query<?>, List<Object[]>> qResult = getResultFromInput(input, queryText1, "");
        try {
          List<Object[]> result = qResult.getRight();
          if (result != null) {
            for (Object[] fields : result) {
              Deadline.checkTimeout();
              Long tabId = Long.parseLong(String.valueOf(fields[0]));
              MetadataTableSummary summary = summaries.get(tabId);
              String transactionalProperties = String.valueOf(fields[1]);
              if("insert_only".equalsIgnoreCase(transactionalProperties.trim())) {
                summary.setTableType("HIVE_ACID_INSERT_ONLY");
              }
            }
          }
        } finally {
          qResult.getLeft().closeAll();
        }
        return Collections.emptyList();
      }
    }.runBatched(batchSize, transactionTables);
  }

  private Pair<Query<?>, List<Object[]>> getResultFromInput(List<Long> input,
      String queryText0, String subQ) throws MetaException {
    int size = input.size();
    String queryText = queryText0 + (size == 0 ? "" : repeat(",?", size).substring(1)) + ") " + subQ;
    if (dbType.isMYSQL()) {
      queryText = queryText.replace("\"", "");
    }
    Object[] params = new Object[size];
    for (int i = 0; i < input.size(); ++i) {
      params[i] = input.get(i);
    }
    Deadline.checkTimeout();
    Query<?> query = pm.newQuery("javax.jdo.query.SQL", queryText);
    List<Object[]> result = (List<Object[]>) query.executeWithArray(params);
    return Pair.of(query, result);
  }

  private void collectTabFormatSummary(List<Long> transactionalTables, Long tableId,
      MetadataTableSummary summary, String slib, String compressionType) {
    String tblType = summary.getTableType();
    String fileType = TableFormat.extractFileFormat(slib);
    Set<String> nonNativeTabTypes = TableFormat.getNonNativeFormats();
    if (nonNativeTabTypes.contains(fileType)) {
      tblType = fileType.toUpperCase();
    } else if (TableType.MANAGED_TABLE.name().equalsIgnoreCase(tblType)) {
      tblType = "HIVE_ACID_FULL";
      transactionalTables.add(tableId);
    } else if (TableType.EXTERNAL_TABLE.name().equalsIgnoreCase(tblType)) {
      tblType = "HIVE_EXTERNAL";
    } else {
      tblType = tblType != null ? tblType.toUpperCase() : "NULL";
    }
    if (compressionType.equals("0") || compressionType.equals("f")) {
      compressionType = "None";
    }
    summary.tableFormatSummary(tblType, compressionType, fileType);
  }

  private void collectBasicStats(Map<Long, MetadataTableSummary> summaries, Set<Long> nonPartedTabs,
      Set<Long> partedTabs) throws MetaException {
    String queryText0 = "select \"TBL_ID\", \"PARAM_KEY\", CAST(" + dbType.toVarChar("\"PARAM_VALUE\"") + " AS decimal(21,0)) from \"TABLE_PARAMS\" where \"PARAM_KEY\" " +
        "in ('" + StatsSetupConst.TOTAL_SIZE + "', '" + StatsSetupConst.NUM_FILES + "', '" + StatsSetupConst.ROW_COUNT + "') and \"TBL_ID\" in (";
    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        collectBasicStats(queryText0, input, summaries, "");
        return Collections.emptyList();
      }
    }.runBatched(batchSize, new ArrayList<>(nonPartedTabs));

   String queryText1 = "select \"TBL_ID\", \"PARAM_KEY\", sum(CAST(" + dbType.toVarChar("\"PARAM_VALUE\"") + " AS decimal(21,0))) from \"PARTITIONS\" t " +
       "join \"PARTITION_PARAMS\" p on p.\"PART_ID\" = t.\"PART_ID\" where \"PARAM_KEY\" " +
       "in ('" + StatsSetupConst.TOTAL_SIZE + "', '" + StatsSetupConst.NUM_FILES + "', '" + StatsSetupConst.ROW_COUNT + "') and t.\"TBL_ID\" in (";
    new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        collectBasicStats(queryText1, input, summaries, " group by \"TBL_ID\", \"PARAM_KEY\"");
        return Collections.emptyList();
      }
    }.runBatched(batchSize, new ArrayList<>(partedTabs));
  }

  private void collectBasicStats(String queryText0, List<Long> input, Map<Long, MetadataTableSummary> summaries, String subQ)
      throws MetaException {
    Pair<Query<?>, List<Object[]>> qResult = getResultFromInput(input, queryText0, subQ);
    try {
      List<Object[]> result = qResult.getRight();
      if (result != null) {
        for (Object[] fields : result) {
          Deadline.checkTimeout();
          Long tabId = Long.parseLong(String.valueOf(fields[0]));
          MetadataTableSummary summary = summaries.get(tabId);
          feedBasicStats(summary, String.valueOf(fields[1]), fields[2]);
        }
      }
    } finally {
      qResult.getLeft().closeAll();
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
      summary.setTotalSize(val);
      break;
    case StatsSetupConst.ROW_COUNT:
      summary.setNumRows(val);
      break;
    case StatsSetupConst.NUM_FILES:
      summary.setNumFiles(val);
      break;
    default:
      throw new AssertionError("This should never happen!");
    }
  }

  public Set<Long> filterTablesForSummary(List<MetadataTableSummary> tableSummaries,
      Integer lastUpdatedDays, Integer tablesLimit) throws MetaException {
    if (tableSummaries == null || tableSummaries.isEmpty() || (tablesLimit != null && tablesLimit == 0)) {
      return Collections.emptySet();
    }
    Set<Long> tableIds = tableSummaries.stream().map(MetadataTableSummary::getTableId).collect(Collectors.toSet());
    if (lastUpdatedDays == null && (tablesLimit == null || tableIds.size() < tablesLimit)) {
      return tableIds;
    }
    String tableType = tableSummaries.get(0).getTableType();
    if (!TableFormat.isIcebergFormat(tableType)) {
      // we don't support filtering this type yet, ignore...
      LOG.warn("This table type: {} hasn't been supported selecting the summary yet, ignore...", tableType);
      return tableIds;
    }

    Deadline.checkTimeout();
    List<Long> tables = new Batchable<Long, Long>() {
      @Override
      public List<Long> run(List<Long> input) throws Exception {
        int size = input.size();
        String queryText =
            "\"TBL_ID\" from \"TABLE_PARAMS\" where \"PARAM_KEY\" = 'current-snapshot-timestamp-ms' "
                + (lastUpdatedDays != null ? (" and CAST(" + dbType.toVarChar("\"PARAM_VALUE\"") + " AS decimal(21,0)) > " + (System.currentTimeMillis() - lastUpdatedDays * 24 * 3600000L)) : "")
                + " and \"TBL_ID\" in (" +  (size == 0 ? "" : repeat(",?", size).substring(1)) + ") "
                + " order by CAST(" + dbType.toVarChar("\"PARAM_VALUE\"") + " AS decimal(21,0)) DESC";
        if (tablesLimit != null && tablesLimit >= 0) {
          queryText = sqlGenerator.addLimitClause(tablesLimit, queryText);
        } else {
          queryText = "select " + queryText;
        }
        if (dbType.isMYSQL()) {
          queryText = queryText.replace("\"", "");
        }
        Object[] params = new Object[size];
        for (int i = 0; i < input.size(); ++i) {
          params[i] = input.get(i);
        }
        Deadline.checkTimeout();
        Query<?> query = pm.newQuery("javax.jdo.query.SQL", queryText);
        List<Long> ids = new ArrayList<>();
        try {
          List<Object> result = (List<Object>) query.executeWithArray(params);
          if (result != null) {
            for (Object fields : result) {
              ids.add(Long.parseLong(fields.toString()));
            }
          }
        } finally {
          query.closeAll();
        }
        return ids;
      }
    }.runBatched(batchSize, new ArrayList<>(tableIds));
    return new HashSet<>(tables);
  }
}
