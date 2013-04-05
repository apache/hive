package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeQueryContextWithStorage extends CubeQueryContext {

  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  private final Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factStorageMap =
      new HashMap<CubeFactTable, Map<UpdatePeriod,List<String>>>();
  private final Map<CubeDimensionTable, List<String>> dimStorageMap =
      new HashMap<CubeDimensionTable, List<String>>();
  private final Map<String, String> storageTableToWhereClause =
      new HashMap<String, String>();
  private final Map<AbstractCubeTable, String> storageTableToQuery =
      new HashMap<AbstractCubeTable, String>();

  public boolean hasPartitions() {
    return !storageTableToWhereClause.isEmpty();
  }

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

  public List<String> getSupportedStorages() {
    return supportedStorages;
  }

  public boolean isStorageSupported(String storage) {
    if (!allStoragesSupported) {
      return supportedStorages.contains(storage);
    }
    return true;
  }

  private final String baseQueryFormat = "SELECT %s FROM %s";

  String getQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(baseQueryFormat);
    if (getWhereTree() != null || hasPartitions()) {
      queryFormat.append(" WHERE %s");
    }
    if (getGroupbyTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (getOrderbyTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  private Object[] getQueryTreeStrings(String factStorageTable) {
    List<String> qstrs = new ArrayList<String>();
    qstrs.add(HQLParser.getString(getSelectTree()));
    String fromString = HQLParser.getString(getFromTree()).toLowerCase();
    String whereString = getWhereTree(factStorageTable);
    for (Map.Entry<AbstractCubeTable, String> entry :
        storageTableToQuery.entrySet()) {
      String src = entry.getKey().getName().toLowerCase();
      System.out.println("From string:" + fromString + " src:" + src + " value:" + entry.getValue());
      fromString = fromString.replaceAll(src, entry.getValue() + " " + src);
    }
    qstrs.add(fromString);
    if (whereString != null) {
      qstrs.add(whereString);
    }
    if (getGroupbyTree() != null) {
      qstrs.add(HQLParser.getString(getGroupbyTree()));
    }
    if (getHavingTree() != null) {
      qstrs.add(HQLParser.getString(getHavingTree()));
    }
    if (getOrderbyTree() != null) {
      qstrs.add(HQLParser.getString(getOrderbyTree()));
    }
    if (getLimitValue() != null) {
      qstrs.add(String.valueOf(getLimitValue()));
    }
    return qstrs.toArray(new String[0]);
  }

  private String toHQL(String tableName) {
    String qfmt = getQueryFormat();
    System.out.println("qfmt:" + qfmt);
    return String.format(qfmt, getQueryTreeStrings(tableName));
  }

  public String getWhereTree(String factStorageTable) {
    String originalWhereString = HQLParser.getString(getWhereTree());
    String whereWithoutTimerange;
    if (factStorageTable != null) {
      whereWithoutTimerange = originalWhereString.substring(0,
          originalWhereString.indexOf(CubeQueryContext.TIME_RANGE_FUNC));
    } else {
      whereWithoutTimerange = originalWhereString;
    }
    // add where clause for all dimensions
    for (CubeDimensionTable dim : dimensions) {
      String storageTable = dimStorageMap.get(dim).get(0);
      storageTableToQuery.put(dim, storageTable);
      whereWithoutTimerange += storageTableToWhereClause.get(storageTable);
    }
    if (factStorageTable != null) {
      // add where clause for fact;
      return whereWithoutTimerange + storageTableToWhereClause.get(
          factStorageTable);
    } else {
      return whereWithoutTimerange;
    }
  }

  @Override
  public String toHQL() throws SemanticException {
    CubeFactTable fact = null;
    if (hasCubeInQuery()) {
      if (candidateFactTables.size() > 0) {
        fact = candidateFactTables.iterator().next();
      }
    }
    if (fact == null && !hasDimensionInQuery()) {
      throw new SemanticException("No valid fact table available");
    }

    if (fact != null) {
      Map<UpdatePeriod, List<String>> storageTableMap = factStorageMap.get(fact);
      Map<UpdatePeriod, List<String>> partColMap = factPartitionMap.get(fact);

      StringBuilder query = new StringBuilder();
      Iterator<UpdatePeriod> it = partColMap.keySet().iterator();
      while (it.hasNext()) {
        UpdatePeriod updatePeriod = it.next();
        String storageTable = storageTableMap.get(updatePeriod).get(0);
        storageTableToQuery.put(getCube(), storageTable);
        query.append(toHQL(storageTable));
        if (it.hasNext()) {
          query.append(" UNION ");
        }
      }
      return query.toString();
    } else {
      return toHQL(null);
    }
  }

  public Map<CubeFactTable, Map<UpdatePeriod, List<String>>> getFactStorageMap()
  {
    return factStorageMap;
  }

  public void setFactStorageMap(Map<CubeFactTable,
      Map<UpdatePeriod, List<String>>> factStorageMap) {
    this.factStorageMap.putAll(factStorageMap);
  }

  public void setDimStorageMap(
      Map<CubeDimensionTable, List<String>> dimStorageMap) {
    this.dimStorageMap.putAll(dimStorageMap);
  }

  public Map<CubeDimensionTable, List<String>> getDimStorageMap() {
    return this.dimStorageMap;
  }

  public Map<String, String> getStorageTableToWhereClause() {
    return storageTableToWhereClause;
  }

  public void setStorageTableToWhereClause(Map<String, String> whereClauseMap) {
    storageTableToWhereClause.putAll(whereClauseMap);
  }
}
