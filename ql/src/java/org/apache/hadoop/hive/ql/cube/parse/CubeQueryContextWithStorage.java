package org.apache.hadoop.hive.ql.cube.parse;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeQueryContextWithStorage extends CubeQueryContext {

  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  private Map<CubeFactTable, Map<UpdatePeriod, List<String>>> storageTableMap;

  public CubeQueryContextWithStorage(ASTNode ast, QB qb,
      List<String> supportedStorages) throws SemanticException {
    super(ast, qb, null);
    this.supportedStorages = supportedStorages;
    this.allStoragesSupported = (supportedStorages == null);
  }

  public CubeQueryContextWithStorage(CubeQueryContext cubeql,
      List<String> supportedStorages) {
    super(cubeql);
    this.supportedStorages = supportedStorages;
    this.allStoragesSupported = (supportedStorages == null);
  }

  public List<String> getStorageNames() {
    return supportedStorages;
  }

  public String getStorageFactTable() {
    if (candidateFactTables.size() > 0) {
      return candidateFactTables.iterator().next().getName();
    }
    return null;
  }

  String simpleQueryFormat = "SELECT %s FROM %s WHERE %s";
  String joinQueryFormat = "SELECT %s FROM %s JOIN %s WHERE %s";

  @Override
  public String toHQL() throws SemanticException {
    String fromString = getStorageFactTable();
    if (fromString == null) {
      throw new SemanticException("No valid fact table available");
    }
    String selectString = HQLParser.getString(getSelectTree());
    String whereString = HQLParser.getString(getWhereTree());

    String actualQuery = String.format(simpleQueryFormat, selectString,
        fromString,
        whereString);
    return actualQuery;
  }

  public Map<CubeFactTable, Map<UpdatePeriod, List<String>>> getStorageTableMap() {
    return storageTableMap;
  }

  public void setStorageTableMap(Map<CubeFactTable,
      Map<UpdatePeriod, List<String>>> storageTableMap) {
    this.storageTableMap = storageTableMap;
  }

}
