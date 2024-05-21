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

package org.apache.hadoop.hive.ql.ddl.table.info.desc;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for table describing commands.
 *
 * A query like this will generate a tree as follows
 *   "describe formatted default.maptable partition (b=100) id;"
 * TOK_TABTYPE
 *   TOK_TABNAME --&gt; root for tablename, 2 child nodes mean DB specified
 *     default
 *     maptable
 *   TOK_PARTSPEC  --&gt; root node for partition spec. else columnName
 *     TOK_PARTVAL
 *       b
 *       100
 *   id           --&gt; root node for columnName
 * formatted
 */
@DDLType(types = HiveParser.TOK_DESCTABLE)
public class DescTableAnalyzer extends BaseSemanticAnalyzer {
  public DescTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    ctx.setResFile(ctx.getLocalTmpPath());

    ASTNode tableTypeExpr = (ASTNode) root.getChild(0);

    TableName tableName = getQualifiedTableName((ASTNode)tableTypeExpr.getChild(0));
    // if database is not the one currently using validate database
    if (tableName.getDb() != null) {
      db.validateDatabaseExists(tableName.getDb());
    }
    Table table = getTable(tableName);

    // process the second child, if exists, node to get partition spec(s)
    Map<String, String> partitionSpec = getPartitionSpec(db, tableTypeExpr, tableName);
    if (partitionSpec != null) {
      // validate that partition exists
      PartitionUtils.getPartition(db, table, partitionSpec, true);
    }

    // process the third child node,if exists, to get partition spec(s)
    String columnPath = getColumnPath(tableTypeExpr, tableName, partitionSpec);

    boolean showColStats = false;
    boolean isFormatted = false;
    boolean isExt = false;
    if (root.getChildCount() == 2) {
      int descOptions = root.getChild(1).getType();
      isFormatted = descOptions == HiveParser.KW_FORMATTED;
      isExt = descOptions == HiveParser.KW_EXTENDED;
      // in case of "DESCRIBE FORMATTED tablename column_name" statement, colPath will contain tablename.column_name.
      // If column_name is not specified colPath will be equal to tableName.
      // This is how we can differentiate if we are describing a table or column.
      if (columnPath != null && isFormatted) {
        showColStats = true;
      }
    }

    inputs.add(new ReadEntity(table));

    DescTableDesc desc = new DescTableDesc(ctx.getResFile(), tableName, partitionSpec, columnPath, isExt, isFormatted);
    Task<?> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    String schema = showColStats ? DescTableDesc.COLUMN_STATISTICS_SCHEMA : DescTableDesc.SCHEMA;
    setFetchTask(createFetchTask(schema));
  }

  /**
   * Get the column path.
   * Return column name if exists, column could be DOT separated.
   * Example: lintString.$elem$.myint.
   * Return table name for column name if no column has been specified.
   */
  private String getColumnPath(ASTNode node, TableName tableName, Map<String, String> partitionSpec) {
    // if this ast has only one child, then no column name specified.
    if (node.getChildCount() == 1) {
      return null;
    }

    // Second child node could be partitionSpec or column
    if (node.getChildCount() > 1) {
      ASTNode columnNode = (partitionSpec == null) ? (ASTNode) node.getChild(1) : (ASTNode) node.getChild(2);
      if (columnNode != null) {
        return String.join(".", tableName.getNotEmptyDbTable(), DDLUtils.getFQName(columnNode));
      }
    }

    return null;
  }

  private Map<String, String> getPartitionSpec(Hive db, ASTNode node, TableName tableName) throws SemanticException {
    // if this node has only one child, then no partition spec specified.
    if (node.getChildCount() == 1) {
      return null;
    }

    // if ast has two children the 2nd child could be partition spec or columnName
    // if the ast has 3 children, the second *has to* be partition spec
    if (node.getChildCount() > 2 && (node.getChild(1).getType() != HiveParser.TOK_PARTSPEC)) {
      throw new SemanticException(node.getChild(1).getType() + " is not a partition specification");
    }

    if (node.getChild(1).getType() == HiveParser.TOK_PARTSPEC) {
      ASTNode partNode = (ASTNode) node.getChild(1);

      Table tab = null;
      try {
        tab = db.getTable(tableName.getNotEmptyDbTable());
      } catch (InvalidTableException e) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName.getNotEmptyDbTable()), e);
      } catch (HiveException e) {
        throw new SemanticException(e.getMessage(), e);
      }

      Map<String, String> partitionSpec = null;
      try {
        partitionSpec = getPartSpec(partNode);
        validateUnsupportedPartitionClause(tab, partitionSpec != null && !partitionSpec.isEmpty());
        partitionSpec = getValidatedPartSpec(tab, partNode, db.getConf(), false);
      } catch (SemanticException e) {
        // get exception in resolving partition it could be DESCRIBE table key
        // return null, continue processing for DESCRIBE table key
        return null;
      }

      if (partitionSpec != null) {
        Partition part = null;
        try {
          part = db.getPartition(tab, partitionSpec, false);
        } catch (HiveException e) {
          // if get exception in finding partition it could be DESCRIBE table key
          // return null, continue processing for DESCRIBE table key
          return null;
        }

        if (part == null) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partitionSpec.toString()));
        }

        return partitionSpec;
      }
    }
    return null;
  }
}