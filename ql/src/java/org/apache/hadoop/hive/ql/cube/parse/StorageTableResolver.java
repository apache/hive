package org.apache.hadoop.hive.ql.cube.parse;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {
  public static Log LOG = LogFactory.getLog(
      StorageTableResolver.class.getName());

  private final Configuration conf;
  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  CubeMetastoreClient client;
  private final boolean failOnPartialData;
  private final List<String> validFactStorageTables;
  private final List<String> validDimTables;
  private final Map<CubeFactTable, Map<UpdatePeriod, List<String>>>
  factStorageMap =
  new HashMap<CubeFactTable, Map<UpdatePeriod, List<String>>>();
  private final Map<CubeFactTable, Map<UpdatePeriod, List<String>>>
  factPartMap =
  new HashMap<CubeFactTable, Map<UpdatePeriod, List<String>>>();
  private final Map<CubeDimensionTable, List<String>> dimStorageMap =
      new HashMap<CubeDimensionTable, List<String>>();
  private final Map<String, String> storageTableToWhereClause =
      new HashMap<String, String>();
  private final List<String> nonExistingParts = new ArrayList<String>();

  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
    this.failOnPartialData = conf.getBoolean(
        CubeQueryConstants.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String str = conf.get(CubeQueryConstants.VALID_STORAGE_FACT_TABLES);
    validFactStorageTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase()));
    str = conf.get(CubeQueryConstants.VALID_STORAGE_DIM_TABLES);
    validDimTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase()));
  }

  private List<String> getSupportedStorages(Configuration conf) {
    String[] storages = conf.getStrings(
        CubeQueryConstants.DRIVER_SUPPORTED_STORAGES);
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }

  public boolean isStorageSupported(String storage) {
    if (!allStoragesSupported) {
      return supportedStorages.contains(storage);
    }
    return true;
  }

  Map<String, List<String>> storagePartMap =
      new HashMap<String, List<String>>();

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {

    client = cubeql.getMetastoreClient();
    if (!cubeql.getCandidateFactTables().isEmpty()) {
      // resolve storage table names
      resolveFactStorageTableNames(cubeql);
      cubeql.setFactStorageMap(factStorageMap);

      // resolve storage partitions
      resolveFactStoragePartitions(cubeql);
      cubeql.setFactPartitionMap(factPartMap);
    }
    // resolve dimension tables
    resolveDimStorageTablesAndPartitions(cubeql);
    cubeql.setDimStorageMap(dimStorageMap);

    // set storage to whereclause
    cubeql.setStorageTableToWhereClause(storageTableToWhereClause);
  }

  private void resolveDimStorageTablesAndPartitions(CubeQueryContext cubeql) {
    for (CubeDimensionTable dim : cubeql.getDimensionTables()) {
      for (String storage : dim.getStorages()) {
        if (isStorageSupported(storage)) {
          String tableName = MetastoreUtil.getDimStorageTableName(
              dim.getName(), Storage.getPrefix(storage)).toLowerCase();
          if (validDimTables != null && !validDimTables.contains(tableName)) {
            LOG.info("Not considering the dim storage table:" + tableName
                + " as it is not a valid dim storage");
            continue;
          }
          List<String> storageTables = dimStorageMap.get(dim);
          if (storageTables == null) {
            storageTables = new ArrayList<String>();
            dimStorageMap.put(dim, storageTables);
          }
          storageTables.add(tableName);
          if (dim.hasStorageSnapshots(storage)) {
            storageTableToWhereClause.put(tableName,
                getWherePartClause(cubeql.getAliasForTabName(dim.getName()),
                    Storage.getPartitionsForLatest()));
          }
        } else {
          LOG.info("Storage:" + storage + " is not supported");
        }
      }
    }
  }

  private void resolveFactStorageTableNames(CubeQueryContext cubeql) {
    for (Iterator<CubeFactTable> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next();
      Map<UpdatePeriod, List<String>> storageTableMap =
          new HashMap<UpdatePeriod, List<String>>();
      factStorageMap.put(fact, storageTableMap);
      for (Map.Entry<String, List<UpdatePeriod>> entry : fact
          .getUpdatePeriods().entrySet()) {
        String storage = entry.getKey();
        // skip storages that are not supported
        if (!isStorageSupported(storage)) {
          continue;
        }
        for (UpdatePeriod updatePeriod : entry.getValue()) {
          String tableName;
          // skip the update period if the storage is not valid
          if ((tableName = getStorageTableName(fact, updatePeriod, storage))
              == null) {
            continue;
          }
          List<String> storageTables = storageTableMap.get(updatePeriod);
          if (storageTables == null) {
            storageTables = new ArrayList<String>();
            storageTableMap.put(updatePeriod, storageTables);
          }
          storageTables.add(tableName);
        }
      }
      if (storageTableMap.isEmpty()) {
        LOG.info("Not considering the fact table:" + fact + " as it does not" +
            " have any storage tables");
        i.remove();
      }
    }
  }

  private void resolveFactStoragePartitions(CubeQueryContext cubeql)
      throws SemanticException {
    Date fromDate = cubeql.getFromDate();
    Date toDate = cubeql.getToDate();

    // Find candidate tables wrt supported storages
    for (Iterator<CubeFactTable> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next();
      Map<UpdatePeriod, List<String>> partitionColMap = getPartitionColMap(fact,
          fromDate, toDate);
      if (partitionColMap == null) {
        LOG.info("Not considering the fact table:" + fact + " as it could not" +
            " find partition for given range:" + fromDate + " - " + toDate);
        i.remove();
        continue;
      }
      factPartMap.put(fact, partitionColMap);
      Map<UpdatePeriod, List<String>> storageTblMap = factStorageMap.get(fact);
      for (UpdatePeriod updatePeriod : partitionColMap.keySet()) {
        List<String> parts = partitionColMap.get(updatePeriod);
        LOG.info("For fact:" + fact + " updatePeriod:" + updatePeriod
            + " Parts:" + parts + " storageTables:"
            + storageTblMap.get(updatePeriod));
        for (String storageTableName : storageTblMap.get(updatePeriod)) {
          storageTableToWhereClause.put(storageTableName, getWherePartClause(
              cubeql.getAliasForTabName(fact.getCubeName()), parts));
        }
      }
    }
  }

  private Map<UpdatePeriod, List<String>> getPartitionColMap(CubeFactTable fact,
      Date fromDate, Date toDate)
          throws SemanticException {
    Map<UpdatePeriod, List<String>> partitionColMap =
        new HashMap<UpdatePeriod, List<String>>();
    Set<UpdatePeriod> updatePeriods = factStorageMap.get(fact).keySet();
    try {
      if (!getPartitions(fact, fromDate, toDate, partitionColMap,
          updatePeriods, true)) {
        return null;
      }
    } catch (HiveException e) {
      new SemanticException(e);
    }
    return partitionColMap;
  }

  String getStorageTableName(CubeFactTable fact, UpdatePeriod updatePeriod,
      String storage) {
    String tableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), updatePeriod, Storage.getPrefix(storage))
        .toLowerCase();
    if (validFactStorageTables != null && !validFactStorageTables
        .contains(tableName)) {
      return null;
    }
    return tableName;
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate,
      Map<UpdatePeriod, List<String>> partitionColMap,
      Set<UpdatePeriod> updatePeriods, boolean addNonExistingParts)
          throws HiveException {
    LOG.info("getPartitions for " + fact + " from fromDate:" + fromDate + " toDate:" + toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }

    UpdatePeriod interval = CubeFactTable.maxIntervalInRange(fromDate, toDate,
        updatePeriods);
    LOG.info("Max interval for " + fact + interval);
    if (interval == null) {
      return false;
    }

    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);
    List<String> storageTbls = factStorageMap.get(fact).get(interval);

    // add partitions from ceilFrom to floorTo
    String fmt = interval.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(ceilFromDate);
    List<String> partitions = new ArrayList<String>();
    Date dt = cal.getTime();
    while (dt.compareTo(floorToDate) < 0) {
      String part = new SimpleDateFormat(fmt).format(cal.getTime());
      cal.add(interval.calendarField(), 1);
      boolean foundPart = false;
      for (String storageTableName : storageTbls) {
        if (client.partitionExists(storageTableName,
            interval, dt)) {
          partitions.add(part);
          foundPart = true;
          break;
        }
      }
      if (!foundPart) {
        LOG.info("Partition:" + part + " does not exist in any storage table");
        Set<UpdatePeriod> newset = new HashSet<UpdatePeriod>();
        newset.addAll(updatePeriods);
        newset.remove(interval);
        if (!getPartitions(fact, dt, cal.getTime(),
            partitionColMap, newset, false)) {
          if (!failOnPartialData && addNonExistingParts) {
            LOG.info("Adding non existing partition" + part);
            partitions.add(part);
            nonExistingParts.add(part);
            foundPart = true;
          } else {
            LOG.info("No finer granual partitions exist for" + part);
            return false;
          }
        } else {
          LOG.info("Finer granual partitions added for " + part);
        }
      }
      dt = cal.getTime();
    }
    List<String> parts = partitionColMap.get(interval);
    if (parts == null) {
      parts = new ArrayList<String>();
      partitionColMap.put(interval, parts);
    }
    parts.addAll(partitions);
    return (getPartitions(fact, fromDate, ceilFromDate, partitionColMap,
        updatePeriods, addNonExistingParts) && getPartitions(fact, floorToDate,
            toDate, partitionColMap, updatePeriods, addNonExistingParts));
  }

  private static String getWherePartClause(String tableName,
      List<String> parts) {
    if (parts.size() == 0) {
      return "";
    }
    StringBuilder partStr = new StringBuilder();
    for (int i = 0; i < parts.size() - 1; i++) {
      partStr.append(tableName);
      partStr.append(".");
      partStr.append(Storage.getDatePartitionKey());
      partStr.append(" = '");
      partStr.append(parts.get(i));
      partStr.append("'");
      partStr.append(" OR ");
    }

    // add the last partition
    partStr.append(tableName);
    partStr.append(".");
    partStr.append(Storage.getDatePartitionKey());
    partStr.append(" = '");
    partStr.append(parts.get(parts.size() - 1));
    partStr.append("'");
    return partStr.toString();
  }
}
