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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * ColumnStatsAutoGatherContext: This is passed to the compiler when set
 * hive.stats.autogather=true during the INSERT OVERWRITE command.
 *
 **/

public class ColumnStatsAutoGatherContext {

  public AnalyzeRewriteContext analyzeRewrite;
  private final List<LoadFileDesc> loadFileWork = new ArrayList<>();
  private final SemanticAnalyzer sa;
  private final HiveConf conf;
  private final Operator<? extends OperatorDesc> op;
  private final List<FieldSchema> columns;
  private final List<FieldSchema> partitionColumns;
  private boolean isInsertInto;
  private Table tbl;
  private Map<String, String> partSpec;
  private Context origCtx;
  
  public ColumnStatsAutoGatherContext(SemanticAnalyzer sa, HiveConf conf,
      Operator<? extends OperatorDesc> op, Table tbl, Map<String, String> partSpec,
      boolean isInsertInto, Context ctx) throws SemanticException {
    super();
    this.sa = sa;
    this.conf = conf;
    this.op = op;
    this.tbl = tbl;
    this.partSpec = partSpec;
    this.isInsertInto = isInsertInto;
    this.origCtx = ctx;
    columns = tbl.getCols();
    partitionColumns = tbl.getPartCols();
  }

  public List<LoadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  public AnalyzeRewriteContext getAnalyzeRewrite() {
    return analyzeRewrite;
  }

  public void setAnalyzeRewrite(AnalyzeRewriteContext analyzeRewrite) {
    this.analyzeRewrite = analyzeRewrite;
  }

  public void insertAnalyzePipeline() throws SemanticException{
    // 1. Generate the statement of analyze table [tablename] compute statistics for columns
    // In non-partitioned table case, it will generate TS-SEL-GBY-RS-GBY-SEL-FS operator
    // In static-partitioned table case, it will generate TS-FIL(partitionKey)-SEL-GBY(partitionKey)-RS-GBY-SEL-FS operator
    // In dynamic-partitioned table case, it will generate TS-SEL-GBY(partitionKey)-RS-GBY-SEL-FS operator
    // However, we do not need to specify the partition-spec because (1) the data is going to be inserted to that specific partition
    // (2) we can compose the static/dynamic partition using a select operator in replaceSelectOperatorProcess..
    String analyzeCommand = "analyze table `" + tbl.getDbName() + "`.`" + tbl.getTableName() + "`"
        + " compute statistics for columns ";

    // 2. Based on the statement, generate the selectOperator
    Operator<?> selOp = null;
    try {
      selOp = genSelOpForAnalyze(analyzeCommand, origCtx);
    } catch (IOException | ParseException e) {
      throw new SemanticException(e);
    }

    // 3. attach this SEL to the operator right before FS
    op.getChildOperators().add(selOp);
    selOp.getParentOperators().clear();
    selOp.getParentOperators().add(op);

    // 4. address the colExp, colList, etc for the SEL
    try {
      replaceSelectOperatorProcess((SelectOperator)selOp, op);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }
  
  @SuppressWarnings("rawtypes")
  private Operator genSelOpForAnalyze(String analyzeCommand, Context origCtx) throws IOException, ParseException, SemanticException{
    //0. initialization
    Context ctx = new Context(conf);
    ctx.setExplainConfig(origCtx.getExplainConfig());
    ASTNode tree = ParseUtils.parse(analyzeCommand, ctx);

    //1. get the ColumnStatsSemanticAnalyzer
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).build();
    BaseSemanticAnalyzer baseSem = SemanticAnalyzerFactory.get(queryState, tree);
    ColumnStatsSemanticAnalyzer colSem = (ColumnStatsSemanticAnalyzer) baseSem;

    //2. get the rewritten AST
    ASTNode ast = colSem.rewriteAST(tree, this);
    baseSem = SemanticAnalyzerFactory.get(queryState, ast);
    SemanticAnalyzer sem = (SemanticAnalyzer) baseSem;
    QB qb = new QB(null, null, false);
    ASTNode child = ast;
    ParseContext subPCtx = ((SemanticAnalyzer) sem).getParseContext();
    subPCtx.setContext(ctx);
    ((SemanticAnalyzer) sem).initParseCtx(subPCtx);
    sem.doPhase1(child, qb, sem.initPhase1Ctx(), null);
    // This will trigger new calls to metastore to collect metadata
    // TODO: cache the information from the metastore
    sem.getMetaData(qb);
    Operator<?> operator = sem.genPlan(qb);

    //3. populate the load file work so that ColumnStatsTask can work
    loadFileWork.addAll(sem.getLoadFileWork());

    //4. because there is only one TS for analyze statement, we can get it.
    if (sem.topOps.values().size() != 1) {
      throw new SemanticException(
          "ColumnStatsAutoGatherContext is expecting exactly one TS, but finds "
              + sem.topOps.values().size());
    }
    operator = sem.topOps.values().iterator().next();

    //5. get the first SEL after TS
    while(!(operator instanceof SelectOperator)){
      operator = operator.getChildOperators().get(0);
    }
    return operator;
  }

  /**
   * @param operator : the select operator in the analyze statement
   * @param input : the operator right before FS in the insert overwrite statement
   * @throws HiveException 
   */
  private void replaceSelectOperatorProcess(SelectOperator operator, Operator<? extends OperatorDesc> input)
      throws HiveException {
    RowSchema selRS = operator.getSchema();
    ArrayList<ColumnInfo> signature = new ArrayList<>();
    OpParseContext inputCtx = sa.opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    ArrayList<ColumnInfo> columns = inputRR.getColumnInfos();
    ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> columnExprMap =
        new HashMap<String, ExprNodeDesc>();
    // the column positions in the operator should be like this
    // <----non-partition columns---->|<--static partition columns-->|<--dynamic partition columns-->
    //        ExprNodeColumnDesc      |      ExprNodeConstantDesc    |     ExprNodeColumnDesc
    //           from input           |         generate itself      |        from input
    //                                |

    // 1. deal with non-partition columns
    for (int i = 0; i < this.columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      ExprNodeDesc exprNodeDesc = new ExprNodeColumnDesc(col);
      colList.add(exprNodeDesc);
      String internalName = selRS.getColumnNames().get(i);
      columnNames.add(internalName);
      columnExprMap.put(internalName, exprNodeDesc);
      signature.add(selRS.getSignature().get(i));
    }
    // if there is any partition column (in static partition or dynamic
    // partition or mixed case)
    int dynamicPartBegin = -1;
    for (int i = 0; i < partitionColumns.size(); i++) {
      ExprNodeDesc exprNodeDesc = null;
      String partColName = partitionColumns.get(i).getName();
      // 2. deal with static partition columns
      if (partSpec != null && partSpec.containsKey(partColName)
          && partSpec.get(partColName) != null) {
        if (dynamicPartBegin > 0) {
          throw new SemanticException(
              "Dynamic partition columns should not come before static partition columns.");
        }
        exprNodeDesc = new ExprNodeConstantDesc(partSpec.get(partColName));
        TypeInfo srcType = exprNodeDesc.getTypeInfo();
        TypeInfo destType = selRS.getSignature().get(this.columns.size() + i).getType();
        if (!srcType.equals(destType)) {
          // This may be possible when srcType is string but destType is integer
          exprNodeDesc = ParseUtils
              .createConversionCast(exprNodeDesc, (PrimitiveTypeInfo) destType);
        }
      }
      // 3. dynamic partition columns
      else {
        dynamicPartBegin++;
        ColumnInfo col = columns.get(this.columns.size() + dynamicPartBegin);
        TypeInfo srcType = col.getType();
        TypeInfo destType = selRS.getSignature().get(this.columns.size() + i).getType();
        exprNodeDesc = new ExprNodeColumnDesc(col);
        if (!srcType.equals(destType)) {
          exprNodeDesc = ParseUtils
              .createConversionCast(exprNodeDesc, (PrimitiveTypeInfo) destType);
        }
      }
      colList.add(exprNodeDesc);
      String internalName = selRS.getColumnNames().get(this.columns.size() + i);
      columnNames.add(internalName);
      columnExprMap.put(internalName, exprNodeDesc);
      signature.add(selRS.getSignature().get(this.columns.size() + i));
    }
    operator.setConf(new SelectDesc(colList, columnNames));
    operator.setColumnExprMap(columnExprMap);
    selRS.setSignature(signature);
    operator.setSchema(selRS);
  }

  public String getCompleteName() {
    return tbl.getFullyQualifiedName();
  }

  public boolean isInsertInto() {
    return isInsertInto;
  }

  public static boolean canRunAutogatherStats(Operator curr) {
    // check the ObjectInspector
    for (ColumnInfo cinfo : curr.getSchema().getSignature()) {
      if (cinfo.getIsVirtualCol()) {
        return false;
      } else if (cinfo.getObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
        return false;
      } else {
        switch (((PrimitiveTypeInfo) cinfo.getType()).getPrimitiveCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case TIMESTAMP:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case CHAR:
        case VARCHAR:
        case BINARY:
        case DECIMAL:
          // TODO: Support case DATE:
          break;
        default:
          return false;
        }
      }
    }
    return true;
  }

}
