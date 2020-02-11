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

package org.apache.hadoop.hive.ql.ddl.table;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Abstract ancestor of all Alter Table analyzer. The alter table commands have this structure:
 * tableName command partitionSpec?
 */
public abstract class AbstractAlterTableAnalyzer extends BaseSemanticAnalyzer {
  // Equivalent to acidSinks, but for DDL operations that change data.
  private DDLDescWithWriteId ddlDescWithWriteId;

  public AbstractAlterTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    TableName tableName = getQualifiedTableName((ASTNode) root.getChild(0), MetaStoreUtils.getDefaultCatalog(conf));

    ASTNode command = (ASTNode)root.getChild(1);

    Map<String, String> partitionSpec = null;
    ASTNode partitionSpecNode = (ASTNode)root.getChild(2);
    if (partitionSpecNode != null) {
      //  We can use alter table partition rename to convert/normalize the legacy partition
      //  column values. In so, we should not enable the validation to the old partition spec
      //  passed in this command.
      if (command.getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
        partitionSpec = getPartSpec(partitionSpecNode);
      } else {
        partitionSpec = getValidatedPartSpec(getTable(tableName), partitionSpecNode, conf, false);
      }
    }

    analyzeCommand(tableName, partitionSpec, command);
  }

  protected abstract void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException;

  protected void setAcidDdlDesc(DDLDescWithWriteId descWithWriteId) {
    if(this.ddlDescWithWriteId != null) {
      throw new IllegalStateException("ddlDescWithWriteId is already set: " + this.ddlDescWithWriteId);
    }
    this.ddlDescWithWriteId = descWithWriteId;
  }

  @Override
  public DDLDescWithWriteId getAcidDdlDesc() {
    return ddlDescWithWriteId;
  }

  protected void addInputsOutputsAlterTable(TableName tableName, Map<String, String> partitionSpec,
      AbstractAlterTableDesc desc, AlterTableType op, boolean doForceExclusive) throws SemanticException {
    boolean isCascade = desc != null && desc.isCascade();
    boolean alterPartitions = partitionSpec != null && !partitionSpec.isEmpty();
    //cascade only occurs at table level then cascade to partition level
    if (isCascade && alterPartitions) {
      throw new SemanticException(ErrorMsg.ALTER_TABLE_PARTITION_CASCADE_NOT_SUPPORTED, op.getName());
    }

    Table table = getTable(tableName, true);
    // cascade only occurs with partitioned table
    if (isCascade && !table.isPartitioned()) {
      throw new SemanticException(ErrorMsg.ALTER_TABLE_NON_PARTITIONED_TABLE_CASCADE_NOT_SUPPORTED);
    }

    // Determine the lock type to acquire
    WriteEntity.WriteType writeType = doForceExclusive ?
        WriteType.DDL_EXCLUSIVE : determineAlterTableWriteType(table, desc, op);

    if (!alterPartitions) {
      inputs.add(new ReadEntity(table));
      WriteEntity alterTableOutput = new WriteEntity(table, writeType);
      outputs.add(alterTableOutput);
      //do not need the lock for partitions since they are covered by the table lock
      if (isCascade) {
        for (Partition part : PartitionUtils.getPartitions(db, table, partitionSpec, false)) {
          outputs.add(new WriteEntity(part, WriteEntity.WriteType.DDL_NO_LOCK));
        }
      }
    } else {
      ReadEntity re = new ReadEntity(table);
      // In the case of altering a table for its partitions we don't need to lock the table
      // itself, just the partitions.  But the table will have a ReadEntity.  So mark that
      // ReadEntity as no lock.
      re.noLockNeeded();
      inputs.add(re);

      if (AlterTableUtils.isFullPartitionSpec(table, partitionSpec)) {
        // Fully specified partition spec
        Partition part = PartitionUtils.getPartition(db, table, partitionSpec, true);
        outputs.add(new WriteEntity(part, writeType));
      } else {
        // Partial partition spec supplied. Make sure this is allowed.
        if (!AlterTableType.SUPPORT_PARTIAL_PARTITION_SPEC.contains(op)) {
          throw new SemanticException(
              ErrorMsg.ALTER_TABLE_TYPE_PARTIAL_PARTITION_SPEC_NO_SUPPORTED, op.getName());
        } else if (!conf.getBoolVar(HiveConf.ConfVars.DYNAMICPARTITIONING)) {
          throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_DISABLED);
        }

        for (Partition part : PartitionUtils.getPartitions(db, table, partitionSpec, true)) {
          outputs.add(new WriteEntity(part, writeType));
        }
      }
    }

    if (desc != null) {
      validateAlterTableType(table, op, desc.expectView());
    }
  }

  // For the time while all the alter table operations are getting migrated there is a duplication of this method here
  private WriteType determineAlterTableWriteType(Table tab, AbstractAlterTableDesc desc, AlterTableType op) {
    boolean convertingToAcid = false;
    if (desc != null && desc.getProps() != null &&
        Boolean.parseBoolean(desc.getProps().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL))) {
      convertingToAcid = true;
    }
    if (!AcidUtils.isTransactionalTable(tab) && convertingToAcid) {
      // non-acid to transactional conversion (property itself) must be mutexed to prevent concurrent writes.
      // See HIVE-16688 for use cases.
      return WriteType.DDL_EXCLUSIVE;
    }
    return WriteEntity.determineAlterTableWriteType(op);
  }

  protected void validateAlterTableType(Table tbl, AlterTableType op, boolean expectView)
      throws SemanticException {
    if (tbl.isView()) {
      if (!expectView) {
        throw new SemanticException(ErrorMsg.ALTER_COMMAND_FOR_VIEWS.getMsg());
      }

      switch (op) {
      case ADDPARTITION:
      case DROPPARTITION:
      case RENAMEPARTITION:
      case ADDPROPS:
      case DROPPROPS:
      case RENAME:
        // allow this form
        break;
      default:
        throw new SemanticException(ErrorMsg.ALTER_VIEW_DISALLOWED_OP.getMsg(op.toString()));
      }
    } else {
      if (expectView) {
        throw new SemanticException(ErrorMsg.ALTER_COMMAND_FOR_TABLES.getMsg());
      }
    }
    if (tbl.isNonNative() && !AlterTableType.NON_NATIVE_TABLE_ALLOWED.contains(op)) {
      throw new SemanticException(ErrorMsg.ALTER_TABLE_NON_NATIVE.format(
          AlterTableType.NON_NATIVE_TABLE_ALLOWED.toString(), tbl.getTableName()));
    }
  }
}
