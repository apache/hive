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

package org.apache.hadoop.hive.ql.ddl.misc.msck;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.function.AbstractFunctionAnalyzer;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * Analyzer for metastore check commands.
 */
@DDLType(types = HiveParser.TOK_MSCK)
public class MsckAnalyzer extends AbstractFunctionAnalyzer {
  public MsckAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() == 0) {
      throw new SemanticException("MSCK command must have arguments");
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    boolean repair = root.getChild(0).getType() == HiveParser.KW_REPAIR;
    int offset = repair ? 1 : 0;
    String tableName = getUnescapedName((ASTNode) root.getChild(0 + offset));

    boolean addPartitions = true;
    boolean dropPartitions = false;
    if (root.getChildCount() > 1 + offset) {
      addPartitions = isMsckAddPartition(root.getChild(1 + offset).getType());
      dropPartitions = isMsckDropPartition(root.getChild(1 + offset).getType());
    }

    Table table = getTable(tableName);
    Map<Integer, List<ExprNodeGenericFuncDesc>> partitionSpecs = ParseUtils.getFullPartitionSpecs(root, table, conf,
        false);
    byte[] filterExp = null;
    if (partitionSpecs != null & !partitionSpecs.isEmpty()) {
      // expression proxy class needs to be PartitionExpressionForMetastore since we intend to use the
      // filterPartitionsByExpr of PartitionExpressionForMetastore for partition pruning down the line.
      // Bail out early if expressionProxyClass is not configured properly.
      String expressionProxyClass = conf.get(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname());
      if (!PartitionExpressionForMetastore.class.getCanonicalName().equals(expressionProxyClass)) {
        throw new SemanticException("Invalid expression proxy class. The config metastore.expression.proxy needs " +
            "to be set to org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore");
      }
      // fetch the first value of partitionSpecs map since it will always have one key, value pair
      filterExp = SerializationUtilities.serializeObjectWithTypeInformation(
          (Serializable) ((List) partitionSpecs.values().toArray()[0]).get(0));
    }

    if (repair && AcidUtils.isTransactionalTable(table)) {
      outputs.add(new WriteEntity(table, AcidUtils.isLocklessReadsEnabled(table, conf) ? 
          WriteType.DDL_EXCL_WRITE : WriteType.DDL_EXCLUSIVE));
    } else {
      outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_SHARED));
    }
    MsckDesc desc = new MsckDesc(tableName, filterExp, ctx.getResFile(), repair, addPartitions, dropPartitions);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  private boolean isMsckAddPartition(int type) {
    return type == HiveParser.KW_SYNC || type == HiveParser.KW_ADD;
  }

  private boolean isMsckDropPartition(int type) {
    return type == HiveParser.KW_SYNC || type == HiveParser.KW_DROP;
  }
}
