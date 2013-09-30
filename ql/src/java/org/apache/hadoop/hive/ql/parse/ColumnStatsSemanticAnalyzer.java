/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * ColumnStatsSemanticAnalyzer.
 * Handles semantic analysis and rewrite for gathering column statistics both at the level of a
 * partition and a table. Note that table statistics are implemented in SemanticAnalyzer.
 *
 */
public class ColumnStatsSemanticAnalyzer extends SemanticAnalyzer {
  private static final Log LOG = LogFactory
      .getLog(ColumnStatsSemanticAnalyzer.class);

  private ASTNode originalTree;
  private ASTNode rewrittenTree;
  private String rewrittenQuery;

  private Context ctx;
  private boolean isRewritten;

  private boolean isTableLevel;
  private String tableName;
  private List<String> colNames;
  private List<String> colType;
  private String partName;

  private class PartitionList {
    private final String[] partKeys;
    private String[] partKeyTypes;
    private final String[] partValues;
    private int numPartitions;
    private int numPartitionValues;

    PartitionList(int numPartitions) {
      this.numPartitions = numPartitions;
      partKeys = new String[numPartitions];
      partValues = new String[numPartitions];
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    public String[] getPartValues() {
      return partValues;
    }

    public String[] getPartKeys() {
      return partKeys;
    }

    public void addPartValue(String partValue, int index) {
      partValues[index] = new String(partValue);
    }

    public void addPartKey(String partKey, int index) {
      partKeys[index] = new String(partKey);
    }

    public int getNumPartValues() {
      return numPartitionValues;
    }

    public void setNumPartValues(int numPartValues) {
      numPartitionValues = numPartValues;
    }

    public String[] getPartKeyTypes() {
      return partKeyTypes;
    }

    public void setPartKeyTypes(String[] partKeyTypes) {
      this.partKeyTypes = partKeyTypes;
    }

    public void setPartKeyType(String partKeyType, int index) {
      partKeyTypes[index] = partKeyType;
    }
  }

  public ColumnStatsSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  private boolean shouldRewrite(ASTNode tree) {
    boolean rwt = false;
    if (tree.getChildCount() > 1) {
      ASTNode child0 = (ASTNode) tree.getChild(0);
      ASTNode child1;
      if (child0.getToken().getType() == HiveParser.TOK_TAB) {
        child0 = (ASTNode) child0.getChild(0);
        if (child0.getToken().getType() == HiveParser.TOK_TABNAME) {
          child1 = (ASTNode) tree.getChild(1);
          if (child1.getToken().getType() == HiveParser.TOK_TABCOLNAME) {
            rwt = true;
          }
        }
      }
    }
    return rwt;
  }

  private boolean isPartitionLevelStats(ASTNode tree) {
    boolean isPartitioned = false;
    ASTNode child = (ASTNode) tree.getChild(0);
    if (child.getChildCount() > 1) {
      child = (ASTNode) child.getChild(1);
      if (child.getToken().getType() == HiveParser.TOK_PARTSPEC) {
        isPartitioned = true;
      }
    }
    return isPartitioned;
  }

  private String getTableName(ASTNode tree) {
    return getUnescapedName((ASTNode) tree.getChild(0).getChild(0));
  }

  private PartitionList getPartKeyValuePairsFromAST(ASTNode tree) {
    ASTNode child = ((ASTNode) tree.getChild(0).getChild(1));
    int numParts = child.getChildCount();
    PartitionList partList = new PartitionList(numParts);
    String partKey;
    String partValue;
    int numPartValue = 0;
    for (int i = 0; i < numParts; i++) {
      partKey = new String(getUnescapedName((ASTNode) child.getChild(i).getChild(0)));
      if (child.getChild(i).getChildCount() > 1) {
        partValue = new String(getUnescapedName((ASTNode) child.getChild(i).getChild(1)));
        partValue = partValue.replaceAll("'", "");
        numPartValue += 1;
      } else {
        partValue = null;
      }
      partList.addPartKey(partKey, i);
      if (partValue != null) {
        partList.addPartValue(partValue, i);
      }
    }
    partList.setNumPartValues(numPartValue);
    return partList;
  }

  private List<String> getColumnName(ASTNode tree) {
    int numCols = tree.getChild(1).getChildCount();
    List<String> colName = new LinkedList<String>();
    for (int i = 0; i < numCols; i++) {
      colName.add(i, new String(getUnescapedName((ASTNode) tree.getChild(1).getChild(i))));
    }
    return colName;
  }

  private int getNumColumns(ASTNode tree) {
    return tree.getChild(1).getChildCount();
  }

  private void validatePartitionKeys(String tableName, PartitionList partList) throws
    SemanticException {
    Table tbl;
    try {
      tbl = db.getTable(tableName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    List<FieldSchema> partKeys = tbl.getPartitionKeys();
    String[] inputPartKeys = partList.getPartKeys();

    if (inputPartKeys.length != partKeys.size()) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INCORRECT_NUM_PART_KEY.getMsg());
    }

    Map<String, Integer> partKeysMap = new LinkedHashMap<String, Integer>();
    for (int i=0; i <inputPartKeys.length; i++) {
      partKeysMap.put(inputPartKeys[i].toLowerCase(), new Integer(1));
    }
    // Verify that the user specified part keys match the part keys in the table
    for (FieldSchema partKey:partKeys) {
      if (!partKeysMap.containsKey(partKey.getName().toLowerCase())) {
        throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PART_KEY.getMsg());
      }
    }
  }

  private String[] getPartitionKeysType(String tableName, PartitionList partList) throws
    SemanticException {
    Table tbl;
    try {
      tbl = db.getTable(tableName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    List<FieldSchema> partKeys = tbl.getPartitionKeys();
    String[] inputPartKeys = partList.getPartKeys();
    String[] inputPartKeyTypes = new String[inputPartKeys.length];

    for (int i=0; i < inputPartKeys.length; i++) {
      for (FieldSchema partKey:partKeys) {
        if (inputPartKeys[i].equalsIgnoreCase(partKey.getName())) {
          inputPartKeyTypes[i] = new String(partKey.getType());
          break;
         }
      }
    }
    return inputPartKeyTypes;
  }

  private String constructPartitionName(String tableName, PartitionList partList)
    throws SemanticException {
    Table tbl;
    Partition part;
    String[] partKeys = partList.getPartKeys();
    String[] partValues = partList.getPartValues();

    try {
      tbl = db.getTable(tableName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    Map<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i=0; i<partKeys.length; i++) {
      partSpec.put(partKeys[i].toLowerCase(), partValues[i]);
    }

    try {
      part = db.getPartition(tbl, partSpec, false);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partName));
    }

    if (part == null) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PARTITION.getMsg());
    }
    return part.getName();
  }

  private void validatePartitionClause(String tableName, PartitionList partList) throws
    SemanticException {
    int numPartKeys = partList.getNumPartitions();
    int numPartValues = partList.getNumPartValues();
    // Raise error if the user has specified dynamic partitions in the partitioning clause
    if (numPartKeys != numPartValues) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_SYNTAX.getMsg());
    }
    // Validate the user specified partition keys match the partition keys in the table
    validatePartitionKeys(tableName, partList);
  }

  private StringBuilder genPartitionClause(PartitionList partList) throws SemanticException {
    StringBuilder whereClause = new StringBuilder(" where ");
    boolean predPresent = false;
    StringBuilder groupByClause = new StringBuilder(" group by ");
    boolean aggPresent = false;
    StringBuilder retClause = null;
    String[] partKeys = partList.getPartKeys();
    String[] partValues = partList.getPartValues();
    String[] partKeysType = getPartitionKeysType(tableName, partList);

    for (int i = 0; i < partList.getNumPartitions(); i++) {
      if (partValues[i] != null) {
        if (!predPresent) {
          whereClause.append(partKeys[i]);
          whereClause.append(" = ");
          if (partKeysType[i].equalsIgnoreCase("string")) {
            whereClause.append("'");
          }
          whereClause.append(partValues[i]);
          if (partKeysType[i].equalsIgnoreCase("string")) {
            whereClause.append("'");
          }
          predPresent = true;
        } else {
          whereClause.append(" and ");
          whereClause.append(partKeys[i]);
          whereClause.append(" = ");
          if (partKeysType[i].equalsIgnoreCase("string")) {
            whereClause.append("'");
          }
          whereClause.append(partValues[i]);
          if (partKeysType[i].equalsIgnoreCase("string")) {
            whereClause.append("'");
          }
        }
      } else {
        if (!aggPresent) {
          groupByClause.append(partKeys[i]);
          aggPresent = true;
        } else {
          groupByClause.append(",");
          groupByClause.append(partKeys[i]);
        }
      }
    }
    // attach the predicate and group by to the return clause
    if (predPresent) {
      retClause = new StringBuilder(whereClause);
    }
    if (aggPresent) {
      retClause.append(groupByClause);
    }
    return retClause;
  }

  private int getNumBitVectorsForNDVEstimation(HiveConf conf) throws SemanticException {
    int numBitVectors;
    float percentageError = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_NDV_ERROR);

    if (percentageError < 0.0) {
      throw new SemanticException("hive.stats.ndv.error can't be negative");
    } else if (percentageError <= 2.4) {
      numBitVectors = 1024;
      LOG.info("Lowest error achievable is 2.4% but error requested is " + percentageError + "%");
      LOG.info("Choosing 1024 bit vectors..");
    } else if (percentageError <= 3.4 ) {
      numBitVectors = 1024;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 1024 bit vectors..");
    } else if (percentageError <= 4.8) {
      numBitVectors = 512;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 512 bit vectors..");
     } else if (percentageError <= 6.8) {
      numBitVectors = 256;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 256 bit vectors..");
    } else if (percentageError <= 9.7) {
      numBitVectors = 128;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 128 bit vectors..");
    } else if (percentageError <= 13.8) {
      numBitVectors = 64;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 64 bit vectors..");
    } else if (percentageError <= 19.6) {
      numBitVectors = 32;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 32 bit vectors..");
    } else if (percentageError <= 28.2) {
      numBitVectors = 16;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 16 bit vectors..");
    } else if (percentageError <= 40.9) {
      numBitVectors = 8;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 8 bit vectors..");
    } else if (percentageError <= 61.0) {
      numBitVectors = 4;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 4 bit vectors..");
    } else {
      numBitVectors = 2;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 2 bit vectors..");
    }
    return numBitVectors;
  }

  private List<String> getTableColumnType(String tableName, List<String> colNames, int numCols)
      throws SemanticException{
    List<String> colTypes = new LinkedList<String>();
    String colName;
    Table tbl;
    try {
      tbl = db.getTable(tableName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    List<FieldSchema> cols = tbl.getCols();

    for (int i=0; i <numCols; i++) {
      colName = colNames.get(i);
      for (FieldSchema col: cols) {
        if (colName.equalsIgnoreCase(col.getName())) {
          colTypes.add(i, new String(col.getType()));
        }
      }
    }
    return colTypes;
  }

  private List<String> getPartitionColumnType(String tableName, String partName,
    List<String> colNames, int numCols) throws SemanticException {
    List<String> colTypes = new LinkedList<String>();
    String colName;
    Table tbl;
    try {
      tbl = db.getTable(tableName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    List<String> partNames = new ArrayList<String>();
    partNames.add(partName);
    List<Partition> partitionList;

    try {
      partitionList = db.getPartitionsByNames(tbl, partNames);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partName));
    }
    Partition part = partitionList.get(0);
    List<FieldSchema> cols = part.getCols();

    for (int i=0; i <numCols; i++) {
      colName = colNames.get(i);
      for (FieldSchema col: cols) {
        if (colName.equalsIgnoreCase(col.getName())) {
          colTypes.add(i, new String(col.getType()));
        }
      }
    }
    return colTypes;
  }

  private String genRewrittenQuery(List<String> colNames, int numBitVectors, PartitionList partList,
    boolean isPartitionStats) throws SemanticException{
    StringBuilder rewrittenQueryBuilder = new StringBuilder("select ");
    String rewrittenQuery;
    for (int i = 0; i < colNames.size(); i++) {
      if (i > 0) {
        rewrittenQueryBuilder.append(" , ");
      }
      rewrittenQueryBuilder.append("compute_stats(");
      rewrittenQueryBuilder.append(colNames.get(i));
      rewrittenQueryBuilder.append(" , ");
      rewrittenQueryBuilder.append(numBitVectors);
      rewrittenQueryBuilder.append(" )");
    }
    rewrittenQueryBuilder.append(" from ");
    rewrittenQueryBuilder.append(tableName);
    isRewritten = true;

    // If partition level statistics is requested, add predicate and group by as needed to rewritten
    // query
     if (isPartitionStats) {
      rewrittenQueryBuilder.append(genPartitionClause(partList));
    }

    rewrittenQuery = rewrittenQueryBuilder.toString();
    rewrittenQuery = new VariableSubstitution().substitute(conf, rewrittenQuery);
    return rewrittenQuery;
  }

  private ASTNode genRewrittenTree(String rewrittenQuery) throws SemanticException {
    ASTNode rewrittenTree;
    // Parse the rewritten query string
    try {
      ctx = new Context(conf);
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_IO_ERROR.getMsg());
    }
    ctx.setCmd(rewrittenQuery);
    ParseDriver pd = new ParseDriver();

    try {
      rewrittenTree = pd.parse(rewrittenQuery, ctx);
    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_PARSE_ERROR.getMsg());
    }
    rewrittenTree = ParseUtils.findRootNonNullToken(rewrittenTree);
    return rewrittenTree;
  }

  public ColumnStatsSemanticAnalyzer(HiveConf conf, ASTNode tree) throws SemanticException {
    super(conf);
    // check if it is no scan. grammar prevents coexit noscan/columns
    super.processNoScanCommand(tree);
    // check if it is partial scan. grammar prevents coexit partialscan/columns
    super.processPartialScanCommand(tree);
    /* Rewrite only analyze table <> column <> compute statistics; Don't rewrite analyze table
     * command - table stats are collected by the table scan operator and is not rewritten to
     * an aggregation.
     */
    if (shouldRewrite(tree)) {
      tableName = new String(getTableName(tree));
      colNames = getColumnName(tree);
      int numCols = getNumColumns(tree);
      // Save away the original AST
      originalTree = tree;
      boolean isPartitionStats = isPartitionLevelStats(tree);
      PartitionList partList = null;
      checkForPartitionColumns(colNames, getPartitionKeys(tableName));
      validateSpecifiedColumnNames(tableName, colNames);

      if (isPartitionStats) {
        isTableLevel = false;
        partList = getPartKeyValuePairsFromAST(tree);
        validatePartitionClause(tableName, partList);
        partName = constructPartitionName(tableName, partList);
        colType = getPartitionColumnType(tableName, partName, colNames, numCols);
      } else {
        isTableLevel = true;
        colType = getTableColumnType(tableName, colNames, numCols);
      }

      int numBitVectors = getNumBitVectorsForNDVEstimation(conf);
      rewrittenQuery = genRewrittenQuery(colNames, numBitVectors, partList, isPartitionStats);
      rewrittenTree = genRewrittenTree(rewrittenQuery);
    } else {
      // Not an analyze table column compute statistics statement - don't do any rewrites
      originalTree = rewrittenTree = tree;
      rewrittenQuery = null;
      isRewritten = false;
    }
  }

  // fail early if the columns specified for column statistics are not valid
  private void validateSpecifiedColumnNames(String tableName, List<String> specifiedCols)
      throws SemanticException {
    List<FieldSchema> fields = null;
    try {
      fields = db.getTable(tableName).getAllCols();
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }
    List<String> tableCols = Utilities.getColumnNamesFromFieldSchema(fields);

    for(String sc : specifiedCols) {
      if (!tableCols.contains(sc.toLowerCase())) {
        String msg = "'" + sc + "' (possible columns are " + tableCols.toString() + ")";
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(msg));
      }
    }
  }

  private List<String> getPartitionKeys(String tableName) throws SemanticException {
    List<FieldSchema> fields;
    try {
      fields = db.getTable(tableName).getPartitionKeys();
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    return Utilities.getColumnNamesFromFieldSchema(fields);
  }

  private void checkForPartitionColumns(List<String> specifiedCols, List<String> partCols)
      throws SemanticException {
    // Raise error if user has specified partition column for stats
    for (String pc : partCols) {
      for (String sc : specifiedCols) {
        if (pc.equalsIgnoreCase(sc)) {
          throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_COLUMN.getMsg()
              + " [Try removing column '" + sc + "' from column list]");
        }
      }
    }
  }

  @Override
  public void analyze(ASTNode ast, Context origCtx) throws SemanticException {
    QB qb;
    QBParseInfo qbp;

    // initialize QB
    init();

    // Setup the necessary metadata if originating from analyze rewrite
    if (isRewritten) {
      qb = getQB();
      qb.setAnalyzeRewrite(true);
      qbp = qb.getParseInfo();
      qbp.setTableName(tableName);
      qbp.setTblLvl(isTableLevel);

      if (!isTableLevel) {
        qbp.setPartName(partName);
      }
      qbp.setColName(colNames);
      qbp.setColType(colType);
      initCtx(ctx);
      LOG.info("Invoking analyze on rewritten query");
      analyzeInternal(rewrittenTree);
    } else {
      initCtx(origCtx);
      LOG.info("Invoking analyze on original query");
      analyzeInternal(originalTree);
    }
  }
}
