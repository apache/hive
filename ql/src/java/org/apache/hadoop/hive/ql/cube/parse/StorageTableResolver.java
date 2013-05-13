package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {
  public static Log LOG = LogFactory.getLog(StorageTableResolver.class.getName());

  private final Configuration conf;
  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    String str = conf.get(CubeQueryConstants.VALID_STORAGE_FACT_TABLES);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    Map<String, String> storageTableToWhereClause =
        new HashMap<String, String>();

    // resolve fact tables
    Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factStorageMap =
        new HashMap<CubeFactTable, Map<UpdatePeriod, List<String>>>();
    Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factPartMap =
        cubeql.getFactPartitionMap();
    String str = conf.get(CubeQueryConstants.VALID_STORAGE_FACT_TABLES);
    List<String> validFactStorageTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase()));

    // Find candidate tables wrt supported storages
    for (CubeFactTable fact : factPartMap.keySet()) {
      Map<UpdatePeriod, List<String>> storageTableMap =
          new HashMap<UpdatePeriod, List<String>>();
      factStorageMap.put(fact, storageTableMap);
      Map<UpdatePeriod, List<String>> partitionColMap = factPartMap.get(fact);
      for (UpdatePeriod updatePeriod : partitionColMap.keySet()) {
        List<String> storageTables = new ArrayList<String>();
        storageTableMap.put(updatePeriod, storageTables);
        List<String> parts = partitionColMap.get(updatePeriod);
        for (String storage : fact.getStorages()) {
          if (cubeql.isStorageSupported(storage)) {
            String tableName = MetastoreUtil.getFactStorageTableName(
                fact.getName(), updatePeriod, Storage.getPrefix(storage))
                .toLowerCase();
            if (validFactStorageTables != null && !validFactStorageTables
                .contains(tableName)) {
              LOG.info("Not considering the fact storage table:" + tableName
                  + " as it is not a valid fact storage");
              continue;
            }
            storageTables.add(tableName);
            storageTableToWhereClause.put(tableName, getWherePartClause(
                cubeql.getAliasForTabName(fact.getCubeName()), parts));
          } else {
            LOG.info("Storage:" + storage + " is not supported");
          }
        }
      }
    }
    cubeql.setFactStorageMap(factStorageMap);
    for (Iterator<CubeFactTable> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next();
      Map<UpdatePeriod, List<String>> storageTableMap = factStorageMap.get(
          fact);
      Map<UpdatePeriod, List<String>> partColMap = cubeql.getFactPartitionMap()
          .get(fact);
      Iterator<UpdatePeriod> it = partColMap.keySet().iterator();
      while (it.hasNext()) {
        UpdatePeriod updatePeriod = it.next();
        if (storageTableMap.get(updatePeriod) == null ||
            storageTableMap.get(updatePeriod).isEmpty()) {
          LOG.info("Removing fact:" + fact +
              " from candidate fact tables, as it does not have storage tables"
              + " for update period" + updatePeriod);
          i.remove();
          break;
        }
      }
    }

    // resolve dimension tables
    Map<CubeDimensionTable, List<String>> dimStorageMap =
        new HashMap<CubeDimensionTable, List<String>>();
    str = conf.get(CubeQueryConstants.VALID_STORAGE_DIM_TABLES);
    List<String> validDimTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase()));
    for (CubeDimensionTable dim : cubeql.getDimensionTables()) {
      List<String> storageTables = new ArrayList<String>();
      dimStorageMap.put(dim, storageTables);
      for (String storage : dim.getStorages()) {
        if (cubeql.isStorageSupported(storage)) {
          String tableName = MetastoreUtil.getDimStorageTableName(
              dim.getName(), Storage.getPrefix(storage)).toLowerCase();
          if (validDimTables != null && !validDimTables.contains(tableName)) {
            LOG.info("Not considering the dim storage table:" + tableName
                + " as it is not a valid dim storage");
            continue;
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
    cubeql.setDimStorageMap(dimStorageMap);
    cubeql.setStorageTableToWhereClause(storageTableToWhereClause);
  }

  private String getWherePartClause(String tableName, List<String> parts) {
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
