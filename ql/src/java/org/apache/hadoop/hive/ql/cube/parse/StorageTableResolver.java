package org.apache.hadoop.hive.ql.cube.parse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {

  public StorageTableResolver(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    CubeQueryContextWithStorage cubeqlStorage =
        (CubeQueryContextWithStorage) cubeql;
    Map<CubeFactTable, Map<UpdatePeriod, List<String>>> storageTableMap =
        new HashMap<CubeFactTable, Map<UpdatePeriod,List<String>>>();
    //Find candidate tables wrt supported storages

    cubeqlStorage.setStorageTableMap(storageTableMap);
  }

}
