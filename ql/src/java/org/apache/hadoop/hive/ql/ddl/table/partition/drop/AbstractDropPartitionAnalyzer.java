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

package org.apache.hadoop.hive.ql.ddl.table.partition.drop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.Lists;

/**
 * Analyzer for drop partition commands.
 */
abstract class AbstractDropPartitionAnalyzer extends AbstractAlterTableAnalyzer {
  AbstractDropPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {

    boolean ifExists = (command.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null)
        || HiveConf.getBoolVar(conf, ConfVars.DROP_IGNORES_NON_EXISTENT);
    // If the drop has to fail on non-existent partitions, we cannot batch expressions.
    // That is because we actually have to check each separate expression for existence.
    // We could do a small optimization for the case where expr has all columns and all
    // operators are equality, if we assume those would always match one partition (which
    // may not be true with legacy, non-normalized column values). This is probably a
    // popular case but that's kinda hacky. Let's not do it for now.
    boolean canGroupExprs = ifExists;

    boolean mustPurge = (command.getFirstChildWithType(HiveParser.KW_PURGE) != null);
    ReplicationSpec replicationSpec = new ReplicationSpec(command);

    Table table = null;
    try {
      table = getTable(tableName);
    } catch (SemanticException se){
      if (replicationSpec.isInReplicationScope() &&
            (
                (se.getCause() instanceof InvalidTableException)
                || (se.getMessage().contains(ErrorMsg.INVALID_TABLE.getMsg()))
            )){
        // If we're inside a replication scope, then the table not existing is not an error.
        // We just return in that case, no drop needed.
        return;
        // TODO : the contains message check is fragile, we should refactor SemanticException to be
        // queriable for error code, and not simply have a message
        // NOTE : IF_EXISTS might also want to invoke this, but there's a good possibility
        // that IF_EXISTS is stricter about table existence, and applies only to the ptn.
        // Therefore, ignoring IF_EXISTS here.
      } else {
        throw se;
      }
    }
    Map<Integer, List<ExprNodeGenericFuncDesc>> partitionSpecs = getFullPartitionSpecs(command, table, canGroupExprs);
    if (partitionSpecs.isEmpty()) { // nothing to do
      return;
    }

    validateAlterTableType(table, AlterTableType.DROPPARTITION, expectView());
    ReadEntity re = new ReadEntity(table);
    re.noLockNeeded();
    inputs.add(re);

    addTableDropPartsOutputs(table, partitionSpecs.values(), !ifExists);

    AlterTableDropPartitionDesc desc =
        new AlterTableDropPartitionDesc(tableName, partitionSpecs, mustPurge, replicationSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  /**
   * Get the partition specs from the tree. This stores the full specification
   * with the comparator operator into the output list.
   *
   * @return Map of partitions by prefix length. Most of the time prefix length will
   *         be the same for all partition specs, so we can just OR the expressions.
   */
  private Map<Integer, List<ExprNodeGenericFuncDesc>> getFullPartitionSpecs(
      CommonTree ast, Table table, boolean canGroupExprs) throws SemanticException {
    String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    Map<String, String> colTypes = new HashMap<>();
    for (FieldSchema fs : table.getPartitionKeys()) {
      colTypes.put(fs.getName().toLowerCase(), fs.getType());
    }

    Map<Integer, List<ExprNodeGenericFuncDesc>> result = new HashMap<>();
    for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      Tree partSpecTree = ast.getChild(childIndex);
      if (partSpecTree.getType() != HiveParser.TOK_PARTSPEC) {
        continue;
      }

      ExprNodeGenericFuncDesc expr = null;
      Set<String> names = new HashSet<>(partSpecTree.getChildCount());
      for (int i = 0; i < partSpecTree.getChildCount(); ++i) {
        CommonTree partSpecSingleKey = (CommonTree) partSpecTree.getChild(i);
        assert (partSpecSingleKey.getType() == HiveParser.TOK_PARTVAL);
        String key = stripIdentifierQuotes(partSpecSingleKey.getChild(0).getText()).toLowerCase();
        String operator = partSpecSingleKey.getChild(1).getText();
        ASTNode partValNode = (ASTNode)partSpecSingleKey.getChild(2);
        TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
        ExprNodeConstantDesc valExpr =
            (ExprNodeConstantDesc) ExprNodeTypeCheck.genExprNode(partValNode, typeCheckCtx).get(partValNode);
        Object val = valExpr.getValue();

        boolean isDefaultPartitionName = val.equals(defaultPartitionName);

        String type = colTypes.get(key);
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        if (type == null) {
          throw new SemanticException("Column " + key + " not found");
        }
        // Create the corresponding hive expression to filter on partition columns.
        if (!isDefaultPartitionName) {
          if (!valExpr.getTypeString().equals(type)) {
            Converter converter = ObjectInspectorConverters.getConverter(
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valExpr.getTypeInfo()),
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti));
            val = converter.convert(valExpr.getValue());
          }
        }

        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op;
        if (!isDefaultPartitionName) {
          op = PartitionUtils.makeBinaryPredicate(operator, column, new ExprNodeConstantDesc(pti, val));
        } else {
          GenericUDF originalOp = FunctionRegistry.getFunctionInfo(operator).getGenericUDF();
          String fnName;
          if (FunctionRegistry.isEq(originalOp)) {
            fnName = "isnull";
          } else if (FunctionRegistry.isNeq(originalOp)) {
            fnName = "isnotnull";
          } else {
            throw new SemanticException(
                "Cannot use " + operator + " in a default partition spec; only '=' and '!=' are allowed.");
          }
          op = PartitionUtils.makeUnaryPredicate(fnName, column);
        }
        // If it's multi-expr filter (e.g. a='5', b='2012-01-02'), AND with previous exprs.
        expr = (expr == null) ? op : PartitionUtils.makeBinaryPredicate("and", expr, op);
        names.add(key);
      }

      if (expr == null) {
        continue;
      }

      // We got the expr for one full partition spec. Determine the prefix length.
      int prefixLength = calculatePartPrefix(table, names);
      List<ExprNodeGenericFuncDesc> orExpr = result.get(prefixLength);
      // We have to tell apart partitions resulting from spec with different prefix lengths.
      // So, if we already have smth for the same prefix length, we can OR the two.
      // If we don't, create a new separate filter. In most cases there will only be one.
      if (orExpr == null) {
        result.put(prefixLength, Lists.newArrayList(expr));
      } else if (canGroupExprs) {
        orExpr.set(0, PartitionUtils.makeBinaryPredicate("or", expr, orExpr.get(0)));
      } else {
        orExpr.add(expr);
      }
    }
    return result;
  }

  /**
   * Calculates the partition prefix length based on the drop spec.
   * This is used to avoid deleting archived partitions with lower level.
   * For example, if, for A and B key cols, drop spec is A=5, B=6, we shouldn't drop
   * archived A=5/, because it can contain B-s other than 6.
   */
  private int calculatePartPrefix(Table tbl, Set<String> partSpecKeys) {
    int partPrefixToDrop = 0;
    for (FieldSchema fs : tbl.getPartCols()) {
      if (!partSpecKeys.contains(fs.getName())) {
        break;
      }
      ++partPrefixToDrop;
    }
    return partPrefixToDrop;
  }

  protected abstract boolean expectView();

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, throw an error if
   * throwIfNonExistent is true, otherwise ignore it.
   */
  private void addTableDropPartsOutputs(Table tab, Collection<List<ExprNodeGenericFuncDesc>> partitionSpecs,
      boolean throwIfNonExistent) throws SemanticException {
    for (List<ExprNodeGenericFuncDesc> specs : partitionSpecs) {
      for (ExprNodeGenericFuncDesc partitionSpec : specs) {
        List<Partition> parts = new ArrayList<>();

        boolean hasUnknown = false;
        try {
          hasUnknown = db.getPartitionsByExpr(tab, partitionSpec, conf, parts);
        } catch (Exception e) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partitionSpec.getExprString()), e);
        }
        if (hasUnknown) {
          throw new SemanticException("Unexpected unknown partitions for " + partitionSpec.getExprString());
        }

        // TODO: ifExists could be moved to metastore. In fact it already supports that. Check it
        //       for now since we get parts for output anyway, so we can get the error message
        //       earlier... If we get rid of output, we can get rid of this.
        if (parts.isEmpty()) {
          if (throwIfNonExistent) {
            throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partitionSpec.getExprString()));
          }
        }
        for (Partition p : parts) {
          outputs.add(new WriteEntity(p, WriteEntity.WriteType.DDL_EXCLUSIVE));
        }
      }
    }
  }
}
