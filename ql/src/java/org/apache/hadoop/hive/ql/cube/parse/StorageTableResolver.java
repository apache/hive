package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
    CubeQueryContextWithStorage cubeqlStorage =
        (CubeQueryContextWithStorage) cubeql;
    Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factStorageMap =
        new HashMap<CubeFactTable, Map<UpdatePeriod,List<String>>>();
    Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factPartMap =
        cubeql.getFactPartitionMap();
    //Find candidate tables wrt supported storages
    for (CubeFactTable fact : factPartMap.keySet()) {
      Map<UpdatePeriod, List<String>> storageTableMap =
          new HashMap<UpdatePeriod, List<String>>();
      factStorageMap.put(fact, storageTableMap);
      Map<UpdatePeriod, List<String>> partitionColMap = factPartMap.get(fact);
      for (UpdatePeriod updatePeriod : partitionColMap.keySet()) {
        List<String> storageTables = new ArrayList<String>();
        storageTableMap.put(updatePeriod, storageTables);
        for (String storage : fact.getStorages()) {
          storageTables.add(MetastoreUtil.getFactStorageTableName(
              fact.getName(), updatePeriod, Storage.getPrefix(storage)));
        }
      }
    }
    cubeqlStorage.setFactStorageMap(factStorageMap);
  }

}
