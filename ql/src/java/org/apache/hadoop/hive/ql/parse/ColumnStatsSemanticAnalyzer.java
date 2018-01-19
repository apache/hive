/*
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * ColumnStatsSemanticAnalyzer.
 * Handles semantic analysis and rewrite for gathering column statistics both at the level of a
 * partition and a table. Note that table statistics are implemented in SemanticAnalyzer.
 *
 */
public class ColumnStatsSemanticAnalyzer extends SemanticAnalyzer {
  private static final Logger LOG = LoggerFactory
      .getLogger(ColumnStatsSemanticAnalyzer.class);
  static final private LogHelper console = new LogHelper(LOG);

  private ASTNode originalTree;
  private ASTNode rewrittenTree;
  private String rewrittenQuery;

  private Context ctx;
  private boolean isRewritten;

  private boolean isTableLevel;
  private List<String> colNames;
  private List<String> colType;
  private Table tbl;

  public ColumnStatsSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
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
          if (child1.getToken().getType() == HiveParser.KW_COLUMNS) {
            rwt = true;
          }
        }
      }
    }
    return rwt;
  }

  private List<String> getColumnName(ASTNode tree) throws SemanticException{

    switch (tree.getChildCount()) {
      case 2:
       return Utilities.getColumnNamesFromFieldSchema(tbl.getCols());
      case 3:
        int numCols = tree.getChild(2).getChildCount();
        List<String> colName = new LinkedList<String>();
        for (int i = 0; i < numCols; i++) {
          colName.add(i, new String(getUnescapedName((ASTNode) tree.getChild(2).getChild(i))));
        }
        return colName;
      default:
        throw new SemanticException("Internal error. Expected number of children of ASTNode to be"
            + " either 2 or 3. Found : " + tree.getChildCount());
    }
  }

  private void handlePartialPartitionSpec(Map<String,String> partSpec, ColumnStatsAutoGatherContext context) throws
    SemanticException {

    // If user has fully specified partition, validate that partition exists
    int partValsSpecified = 0;
    for (String partKey : partSpec.keySet()) {
      partValsSpecified += partSpec.get(partKey) == null ? 0 : 1;
    }
    try {
      // for static partition, it may not exist when HIVESTATSCOLAUTOGATHER is
      // set to true
      if (context == null) {
        if ((partValsSpecified == tbl.getPartitionKeys().size())
            && (db.getPartition(tbl, partSpec, false, null, false) == null)) {
          throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PARTITION.getMsg()
              + " : " + partSpec);
        }
      }
    } catch (HiveException he) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PARTITION.getMsg() + " : "
          + partSpec);
    }

    // User might have only specified partial list of partition keys, in which case add other partition keys in partSpec
    List<String> partKeys = Utilities.getColumnNamesFromFieldSchema(tbl.getPartitionKeys());
    for (String partKey : partKeys){
     if(!partSpec.containsKey(partKey)) {
       partSpec.put(partKey, null);
     }
   }

   // Check if user have erroneously specified non-existent partitioning columns
   for (String partKey : partSpec.keySet()) {
     if(!partKeys.contains(partKey)){
       throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PART_KEY.getMsg() + " : " + partKey);
     }
   }
  }

  private StringBuilder genPartitionClause(Map<String,String> partSpec) throws SemanticException {
    StringBuilder whereClause = new StringBuilder(" where ");
    boolean predPresent = false;
    StringBuilder groupByClause = new StringBuilder(" group by ");
    boolean aggPresent = false;

    for (String partKey : partSpec.keySet()) {
      String value;
      if ((value = partSpec.get(partKey)) != null) {
        if (!predPresent) {
          predPresent = true;
        } else {
          whereClause.append(" and ");
        }
        whereClause.append("`").append(partKey).append("` = ").append(genPartValueString(partKey, value));
      }
    }

     for (FieldSchema fs : tbl.getPartitionKeys()) {
        if (!aggPresent) {
          aggPresent = true;
        } else {
          groupByClause.append(",");
        }
      groupByClause.append("`" + fs.getName() + "`");
    }

    // attach the predicate and group by to the return clause
    return predPresent ? whereClause.append(groupByClause) : groupByClause;
  }

  private String genPartValueString (String partKey, String partVal) throws SemanticException {
    String returnVal = partVal;
    String partColType = getColTypeOf(partKey);
    if (partColType.equals(serdeConstants.STRING_TYPE_NAME) ||
        partColType.contains(serdeConstants.VARCHAR_TYPE_NAME) ||
        partColType.contains(serdeConstants.CHAR_TYPE_NAME)) {
      returnVal = "'" + escapeSQLString(partVal) + "'";
    } else if (partColType.equals(serdeConstants.TINYINT_TYPE_NAME)) {
      returnVal = partVal+"Y";
    } else if (partColType.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
      returnVal = partVal+"S";
    } else if (partColType.equals(serdeConstants.INT_TYPE_NAME)) {
      returnVal = partVal;
    } else if (partColType.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      returnVal = partVal+"L";
    } else if (partColType.contains(serdeConstants.DECIMAL_TYPE_NAME)) {
      returnVal = partVal + "BD";
    } else if (partColType.equals(serdeConstants.DATE_TYPE_NAME) ||
        partColType.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      returnVal = partColType + " '" + escapeSQLString(partVal) + "'";
    } else {
      //for other usually not used types, just quote the value
      returnVal = "'" + escapeSQLString(partVal) + "'";
    }

    return returnVal;
  }

  private String getColTypeOf (String partKey) throws SemanticException{

    for (FieldSchema fs : tbl.getPartitionKeys()) {
      if (partKey.equalsIgnoreCase(fs.getName())) {
        return fs.getType().toLowerCase();
      }
    }
    throw new SemanticException ("Unknown partition key : " + partKey);
  }

  private List<String> getColumnTypes(List<String> colNames)
      throws SemanticException{
    List<String> colTypes = new ArrayList<String>();
    List<FieldSchema> cols = tbl.getCols();
    List<String> copyColNames = new ArrayList<>();
    copyColNames.addAll(colNames);

    for (String colName : copyColNames) {
      for (FieldSchema col : cols) {
        if (colName.equalsIgnoreCase(col.getName())) {
          String type = col.getType();
          TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
          if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            logTypeWarning(colName, type);
            colNames.remove(colName);
          } else {
            colTypes.add(type);
          }
        }
      }
    }
    
    return colTypes;
  }

  private String escapeBackTicks(String colName) {
    return colName.replaceAll("`", "``");
  }

  private String genRewrittenQuery(List<String> colNames, HiveConf conf, Map<String,String> partSpec,
    boolean isPartitionStats) throws SemanticException{
    StringBuilder rewrittenQueryBuilder = new StringBuilder("select ");
    String rewrittenQuery;

    for (int i = 0; i < colNames.size(); i++) {
      if (i > 0) {
        rewrittenQueryBuilder.append(" , ");
      }
      String func = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_STATS_NDV_ALGO).toLowerCase();
      rewrittenQueryBuilder.append("compute_stats(`");
      rewrittenQueryBuilder.append(escapeBackTicks(colNames.get(i)));
      rewrittenQueryBuilder.append("`, '" + func + "'");
      if (func.equals("fm")) {
        int numBitVectors = 0;
        try {
          numBitVectors = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
        } catch (Exception e) {
          throw new SemanticException(e.getMessage());
        }
        rewrittenQueryBuilder.append(", " + numBitVectors);
      }
      rewrittenQueryBuilder.append(")");
    }

    if (isPartitionStats) {
      for (FieldSchema fs : tbl.getPartCols()) {
        rewrittenQueryBuilder.append(" , `" + fs.getName() + "`");
      }
    }
    rewrittenQueryBuilder.append(" from `");
    rewrittenQueryBuilder.append(tbl.getDbName());
    rewrittenQueryBuilder.append("`.");
    rewrittenQueryBuilder.append("`" + tbl.getTableName() + "`");
    isRewritten = true;

    // If partition level statistics is requested, add predicate and group by as needed to rewritten
    // query
     if (isPartitionStats) {
      rewrittenQueryBuilder.append(genPartitionClause(partSpec));
    }

    rewrittenQuery = rewrittenQueryBuilder.toString();
    rewrittenQuery = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return SessionState.get().getHiveVariables();
      }
    }).substitute(conf, rewrittenQuery);
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

    try {
      rewrittenTree = ParseUtils.parse(rewrittenQuery, ctx);
    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_PARSE_ERROR.getMsg());
    }
    return rewrittenTree;
  }

  // fail early if the columns specified for column statistics are not valid
  private void validateSpecifiedColumnNames(List<String> specifiedCols)
      throws SemanticException {
    List<String> tableCols = Utilities.getColumnNamesFromFieldSchema(tbl.getCols());
    for(String sc : specifiedCols) {
      if (!tableCols.contains(sc.toLowerCase())) {
        String msg = "'" + sc + "' (possible columns are " + tableCols.toString() + ")";
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(msg));
      }
    }
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

  private void logTypeWarning(String colName, String colType) {
    String warning = "Only primitive type arguments are accepted but " + colType
        + " is passed for " + colName + ".";
    warning = "WARNING: " + warning;
    console.printInfo(warning);
  }

  @Override
  public void analyze(ASTNode ast, Context origCtx) throws SemanticException {
    QB qb;
    QBParseInfo qbp;

    // initialize QB
    init(true);

    // check if it is no scan. grammar prevents coexit noscan/columns
    super.processNoScanCommand(ast);
    /* Rewrite only analyze table <> column <> compute statistics; Don't rewrite analyze table
     * command - table stats are collected by the table scan operator and is not rewritten to
     * an aggregation.
     */
    if (shouldRewrite(ast)) {
      tbl = AnalyzeCommandUtils.getTable(ast, this);
      colNames = getColumnName(ast);
      // Save away the original AST
      originalTree = ast;
      boolean isPartitionStats = AnalyzeCommandUtils.isPartitionLevelStats(ast);
      Map<String,String> partSpec = null;
      checkForPartitionColumns(
          colNames, Utilities.getColumnNamesFromFieldSchema(tbl.getPartitionKeys()));
      validateSpecifiedColumnNames(colNames);
      if (conf.getBoolVar(ConfVars.HIVE_STATS_COLLECT_PART_LEVEL_STATS) && tbl.isPartitioned()) {
        isPartitionStats = true;
      }

      if (isPartitionStats) {
        isTableLevel = false;
        partSpec = AnalyzeCommandUtils.getPartKeyValuePairsFromAST(tbl, ast, conf);
        handlePartialPartitionSpec(partSpec, null);
      } else {
        isTableLevel = true;
      }
      colType = getColumnTypes(colNames);
      rewrittenQuery = genRewrittenQuery(colNames, conf, partSpec, isPartitionStats);
      rewrittenTree = genRewrittenTree(rewrittenQuery);
    } else {
      // Not an analyze table column compute statistics statement - don't do any rewrites
      originalTree = rewrittenTree = ast;
      rewrittenQuery = null;
      isRewritten = false;
    }

    // Setup the necessary metadata if originating from analyze rewrite
    if (isRewritten) {
      qb = getQB();
      qb.setAnalyzeRewrite(true);
      qbp = qb.getParseInfo();
      analyzeRewrite = new AnalyzeRewriteContext();
      analyzeRewrite.setTableName(tbl.getFullyQualifiedName());
      analyzeRewrite.setTblLvl(isTableLevel);
      analyzeRewrite.setColName(colNames);
      analyzeRewrite.setColType(colType);
      qbp.setAnalyzeRewrite(analyzeRewrite);
      initCtx(ctx);
      ctx.setExplainConfig(origCtx.getExplainConfig());
      LOG.info("Invoking analyze on rewritten query");
      analyzeInternal(rewrittenTree);
    } else {
      initCtx(origCtx);
      LOG.info("Invoking analyze on original query");
      analyzeInternal(originalTree);
    }
  }

  /**
   * @param ast
   *          is the original analyze ast
   * @param qb
   *          is the qb that calls this function
   * @param sem
   *          is the semantic analyzer that calls this function
   * @return
   * @throws SemanticException
   */
  public ASTNode rewriteAST(ASTNode ast, ColumnStatsAutoGatherContext context)
      throws SemanticException {
    tbl = AnalyzeCommandUtils.getTable(ast, this);
    colNames = getColumnName(ast);
    // Save away the original AST
    originalTree = ast;
    boolean isPartitionStats = AnalyzeCommandUtils.isPartitionLevelStats(ast);
    Map<String, String> partSpec = null;
    checkForPartitionColumns(colNames,
        Utilities.getColumnNamesFromFieldSchema(tbl.getPartitionKeys()));
    validateSpecifiedColumnNames(colNames);
    if (conf.getBoolVar(ConfVars.HIVE_STATS_COLLECT_PART_LEVEL_STATS) && tbl.isPartitioned()) {
      isPartitionStats = true;
    }

    if (isPartitionStats) {
      isTableLevel = false;
      partSpec = AnalyzeCommandUtils.getPartKeyValuePairsFromAST(tbl, ast, conf);
      handlePartialPartitionSpec(partSpec, context);
    } else {
      isTableLevel = true;
    }
    colType = getColumnTypes(colNames);
    rewrittenQuery = genRewrittenQuery(colNames, conf, partSpec, isPartitionStats);
    rewrittenTree = genRewrittenTree(rewrittenQuery);

    context.analyzeRewrite = new AnalyzeRewriteContext();
    context.analyzeRewrite.setTableName(tbl.getFullyQualifiedName());
    context.analyzeRewrite.setTblLvl(isTableLevel);
    context.analyzeRewrite.setColName(colNames);
    context.analyzeRewrite.setColType(colType);
    return rewrittenTree;
  }
}
