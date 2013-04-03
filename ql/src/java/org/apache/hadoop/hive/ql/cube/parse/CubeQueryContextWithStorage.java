package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeQueryContextWithStorage extends CubeQueryContext {

  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  private Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factStorageMap;

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

  public CubeFactTable getCandidateFactTable() {
    if (candidateFactTables.size() > 0) {
      return candidateFactTables.iterator().next();
    }
    return null;
  }

  String simpleQueryFormat = "SELECT %s FROM %s WHERE %s";
  String joinQueryFormat = "SELECT %s FROM %s JOIN %s WHERE %s";

  private String toHQL(CubeFactTable fact) {
    Map<UpdatePeriod, List<String>> storageTableMap = factStorageMap.get(fact);
    Map<UpdatePeriod, List<String>> partColMap = factPartitionMap.get(fact);

    StringBuilder query = new StringBuilder();
    Iterator<UpdatePeriod> it = partColMap.keySet().iterator();
    while (it.hasNext()) {
      UpdatePeriod updatePeriod = it.next();
      query.append(toHQL(storageTableMap.get(updatePeriod).get(0),
          partColMap.get(updatePeriod)));
      if (it.hasNext()) {
        query.append(" UNION ");
      }
    }
    return query.toString();
  }

  private String toHQL(String tableName, List<String> parts) {
    String selectString = HQLParser.getString(getSelectTree());
    String whereString = getWhereTree(parts);
    String actualQuery = String.format(simpleQueryFormat, selectString,
        tableName,
        whereString);
    return actualQuery;
  }

  public String getWhereTree(List<String> parts) {
    //TODO Construct where tree with part conditions
    String originalWhereString = HQLParser.getString(super.getWhereTree());
    String whereWithoutTimerange = originalWhereString.substring(0,
        originalWhereString.indexOf(TIME_RANGE_FUNC));
    String whereWithPartCols = whereWithoutTimerange + getWherePartClause(parts);
    return whereWithPartCols;
  }

  public String getWherePartClause(List<String> parts) {
    StringBuilder partStr = new StringBuilder();
    for (int i = 0; i < parts.size() - 1; i++) {
      partStr.append(Storage.getDatePartitionKey());
      partStr.append(" = '");
      partStr.append(parts.get(i));
      partStr.append("'");
      partStr.append(" OR ");
    }

    // add the last partition
    partStr.append(Storage.getDatePartitionKey());
    partStr.append(" = '");
    partStr.append(parts.get(parts.size() - 1));
    partStr.append("'");
    return partStr.toString();
  }

  @Override
  public String toHQL() throws SemanticException {
    CubeFactTable candidateFactTable = getCandidateFactTable();
    if (candidateFactTable == null) {
      throw new SemanticException("No valid fact table available");
    }
    return toHQL(candidateFactTable);
  }

  public Map<CubeFactTable, Map<UpdatePeriod, List<String>>> getFactStorageMap() {
    return factStorageMap;
  }

  public void setFactStorageMap(Map<CubeFactTable,
      Map<UpdatePeriod, List<String>>> storageTableMap) {
    this.factStorageMap = storageTableMap;
  }

}
