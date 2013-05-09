package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {

  public StorageTableResolver(Configuration conf) {
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
                fact.getName(), updatePeriod, Storage.getPrefix(storage));
            storageTables.add(tableName);
            storageTableToWhereClause.put(tableName,
                getWherePartClause(fact.getCubeName(), parts));
          } else {
            System.out.println("Storage:" + storage + " is not supported");
          }
        }
      }
    }
    cubeql.setFactStorageMap(factStorageMap);

    // resolve dimension tables
    Map<CubeDimensionTable, List<String>> dimStorageMap =
        new HashMap<CubeDimensionTable, List<String>>();
    for (CubeDimensionTable dim : cubeql.getDimensionTables()) {
      List<String> storageTables = new ArrayList<String>();
      dimStorageMap.put(dim, storageTables);
      for (String storage : dim.getStorages()) {
        if (cubeql.isStorageSupported(storage)) {
          String tableName = MetastoreUtil.getDimStorageTableName(
              dim.getName(), Storage.getPrefix(storage));
          storageTables.add(tableName);
          if (dim.hasStorageSnapshots(storage)) {
            storageTableToWhereClause.put(tableName,
                getWherePartClause(dim.getName(), Storage
                    .getPartitionsForLatest()));
          }
        } else {
          System.out.println("Storage:" + storage + " is not supported");
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
