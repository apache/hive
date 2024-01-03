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

package org.apache.hadoop.hive.ql.ddl.table.partition.show;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Analyzer for show partition commands.
 */
@DDLType(types = HiveParser.TOK_SHOWPARTITIONS)
public  class ShowPartitionAnalyzer extends BaseSemanticAnalyzer {
  public ShowPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    ctx.setResFile(ctx.getLocalTmpPath());

    String tableName = getUnescapedName((ASTNode) ast.getChild(0));

    List<Map<String, String>> partSpecs = getPartitionSpecs(getTable(tableName), ast);
    assert (partSpecs.size() <= 1);
    Map<String, String> partSpec = (partSpecs.size() > 0) ? partSpecs.get(0) : null;

    Table table = getTable(HiveTableName.of(tableName));
    inputs.add(new ReadEntity(table));
    setColumnAccessInfo(new ColumnAccessInfo());
    table.getPartColNames().forEach(col -> getColumnAccessInfo().add(table.getCompleteName(), col));

    ExprNodeDesc filter = getShowPartitionsFilter(table, ast);
    String orderBy = getShowPartitionsOrder(table, ast);
    short limit = getShowPartitionsLimit(ast);

    ShowPartitionsDesc desc = new ShowPartitionsDesc(tableName, ctx.getResFile(),
        partSpec, filter, orderBy, limit);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowPartitionsDesc.SCHEMA));
  }


  @VisibleForTesting
  ExprNodeDesc getShowPartitionsFilter(Table table, ASTNode command) throws SemanticException {
    ExprNodeDesc showFilter = null;
    for (int childIndex = 0; childIndex < command.getChildCount(); childIndex++) {
      ASTNode astChild = (ASTNode)command.getChild(childIndex);
      if (astChild.getType() == HiveParser.TOK_WHERE) {
        RowResolver rwsch = new RowResolver();
        Map<String, String> colTypes = new HashMap<String, String>();
        for (FieldSchema fs : table.getPartCols()) {
          rwsch.put(table.getTableName(), fs.getName(), new ColumnInfo(fs.getName(),
              TypeInfoFactory.stringTypeInfo, null, true));
          colTypes.put(fs.getName().toLowerCase(), fs.getType());
        }
        TypeCheckCtx tcCtx = new TypeCheckCtx(rwsch);
        ASTNode conds = (ASTNode) astChild.getChild(0);
        Map<ASTNode, ExprNodeDesc> nodeOutputs = ExprNodeTypeCheck.genExprNode(conds, tcCtx);
        ExprNodeDesc target = nodeOutputs.get(conds);
        if (!(target instanceof ExprNodeGenericFuncDesc) || !target.getTypeInfo().equals(
            TypeInfoFactory.booleanTypeInfo)) {
          String errorMsg = tcCtx.getError() != null ? ". " + tcCtx.getError() : "";
          throw new SemanticException("Not a filter expr: " +
              (target == null ? "null" : target.getExprString()) + errorMsg);
        }

        showFilter = replaceDefaultPartNameAndCastType(target, colTypes,
            HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULT_PARTITION_NAME));
      }
    }
    return showFilter;
  }

  private ExprNodeDesc replaceDefaultPartNameAndCastType(ExprNodeDesc nodeDesc,
      Map<String, String> colTypes, String defaultPartName) throws SemanticException {
    if (!(nodeDesc instanceof ExprNodeGenericFuncDesc)) {
      return nodeDesc;
    }
    ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) nodeDesc;
    if (FunctionRegistry.isOpAnd(funcDesc) || FunctionRegistry.isOpOr(funcDesc)) {
      List<ExprNodeDesc> newChildren = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc child : funcDesc.getChildren()) {
        newChildren.add(replaceDefaultPartNameAndCastType(child, colTypes, defaultPartName));
      }
      funcDesc.setChildren(newChildren);
      return funcDesc;
    }

    List<ExprNodeDesc> children = funcDesc.getChildren();
    int colIdx = -1, constIdx = -1;
    for (int i = 0; i < children.size(); i++) {
      ExprNodeDesc child = children.get(i);
      if (child instanceof ExprNodeColumnDesc) {
        String col = ((ExprNodeColumnDesc)child).getColumn().toLowerCase();
        String type = colTypes.get(col);
        if (!type.equals(child.getTypeString())) {
          child.setTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo(type));
        }
        colIdx = i;
      } else if (child instanceof ExprNodeConstantDesc) {
        constIdx = i;
      }
    }

    if (funcDesc.getGenericUDF() instanceof GenericUDFBaseCompare && children.size() == 2 &&
        colIdx > -1 && constIdx > -1) {
      ExprNodeConstantDesc constantDesc = (ExprNodeConstantDesc)children.get(constIdx);
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc)children.get(colIdx);
      Object val = constantDesc.getValue();
      boolean isDefaultPartitionName = defaultPartName.equals(val);
      String type = colTypes.get(columnDesc.getColumn().toLowerCase());
      PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
      if (!isDefaultPartitionName) {
        if (!constantDesc.getTypeString().equals(type)) {
          Object converted = ObjectInspectorConverters.getConverter(
              TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(constantDesc.getTypeInfo()),
              TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti))
              .convert(val);
          if (converted == null) {
            throw new SemanticException("Cannot convert to " + type + " from " +
                constantDesc.getTypeString() + ", value: " + val);
          }
          ExprNodeConstantDesc newConstantDesc = new ExprNodeConstantDesc(pti, converted);
          children.set(constIdx, newConstantDesc);
        }
      } else {
        GenericUDF originalOp = funcDesc.getGenericUDF();
        String fnName;
        if (FunctionRegistry.isEq(originalOp)) {
          fnName = "isnull";
        } else if (FunctionRegistry.isNeq(originalOp)) {
          fnName = "isnotnull";
        } else {
          throw new SemanticException(
              "Only '=' and '!=' are allowed for the default partition, function: " + originalOp.getUdfName());
        }
        funcDesc = PartitionUtils.makeUnaryPredicate(fnName, columnDesc);
      }
    }

    return funcDesc;
  }

  private String getShowPartitionsOrder(Table table, ASTNode command) throws SemanticException {
    String orderBy = null;
    for (int childIndex = 0; childIndex < command.getChildCount(); childIndex++) {
      ASTNode astChild = (ASTNode) command.getChild(childIndex);
      if (astChild.getType() == HiveParser.TOK_ORDERBY) {
        Map<String,  Integer> poses = new HashMap<String, Integer>();
        RowResolver rwsch = new RowResolver();
        for (int i = 0; i < table.getPartCols().size(); i++) {
          FieldSchema fs = table.getPartCols().get(i);
          rwsch.put(table.getTableName(), fs.getName(), new ColumnInfo(fs.getName(),
              TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()), null, true));
          poses.put(fs.getName().toLowerCase(), i);
        }
        TypeCheckCtx tcCtx = new TypeCheckCtx(rwsch);

        StringBuilder colIndices = new StringBuilder();
        StringBuilder order = new StringBuilder();
        int ccount = astChild.getChildCount();
        for (int i = 0; i < ccount; ++i) {
          // @TODO: implement null first or last
          ASTNode cl = (ASTNode) astChild.getChild(i);
          if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
            order.append("+");
            cl = (ASTNode) cl.getChild(0).getChild(0);
          } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
            order.append("-");
            cl = (ASTNode) cl.getChild(0).getChild(0);
          } else {
            order.append("+");
          }
          Map<ASTNode, ExprNodeDesc> nodeOutputs =  ExprNodeTypeCheck.genExprNode(cl, tcCtx);
          ExprNodeDesc desc = nodeOutputs.get(cl);
          if (!(desc instanceof ExprNodeColumnDesc)) {
            throw new SemanticException("Only partition keys are allowed for " +
                "sorting partition names, input: " + cl.toStringTree());
          }
          String col = ((ExprNodeColumnDesc) desc).getColumn().toLowerCase();
          colIndices.append(poses.get(col)).append(",");
        }
        colIndices.setLength(colIndices.length() - 1);
        orderBy = colIndices + ":" + order;
      }
    }
    return orderBy;
  }

  private short getShowPartitionsLimit(ASTNode command) {
    short limit = -1;
    for (int childIndex = 0; childIndex < command.getChildCount(); childIndex++) {
      ASTNode astChild = (ASTNode) command.getChild(childIndex);
      if (astChild.getType() == HiveParser.TOK_LIMIT) {
        limit = Short.valueOf((astChild.getChild(0)).getText());
      }
    }
    return limit;
  }
}
