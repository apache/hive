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
package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.AlterTablePartMergeFilesDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;
import java.util.HashSet;

/**
 * DDLWork.
 *
 */
public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;

  // TODO: this can probably be replaced with much less code via dynamic dispatch and/or templates.
  private InsertCommitHookDesc insertCommitHookDesc;
  private AlterMaterializedViewDesc alterMVDesc;
  private CreateViewDesc createVwDesc;
  private DropPartitionDesc dropPartitionDesc;
  private AlterTableDesc alterTblDesc;
  private ShowColumnsDesc showColumnsDesc;
  private ShowFunctionsDesc showFuncsDesc;
  private ShowLocksDesc showLocksDesc;
  private ShowCompactionsDesc showCompactionsDesc;
  private ShowTxnsDesc showTxnsDesc;
  private AbortTxnsDesc abortTxnsDesc;
  private DescFunctionDesc descFunctionDesc;
  private ShowPartitionsDesc showPartsDesc;
  private AddPartitionDesc addPartitionDesc;
  private RenamePartitionDesc renamePartitionDesc;
  private AlterTableSimpleDesc alterTblSimpleDesc;
  private MsckDesc msckDesc;
  private AlterTableAlterPartDesc alterTableAlterPartDesc;
  private AlterTableExchangePartition alterTableExchangePartition;
  private KillQueryDesc killQueryDesc;

  private RoleDDLDesc roleDDLDesc;
  private GrantDesc grantDesc;
  private ShowGrantDesc showGrantDesc;
  private RevokeDesc revokeDesc;
  private GrantRevokeRoleDDL grantRevokeRoleDDL;

  private ShowConfDesc showConfDesc;

  private CreateResourcePlanDesc createResourcePlanDesc;
  private ShowResourcePlanDesc showResourcePlanDesc;
  private DropResourcePlanDesc dropResourcePlanDesc;
  private AlterResourcePlanDesc alterResourcePlanDesc;

  private CreateWMTriggerDesc createWMTriggerDesc;
  private AlterWMTriggerDesc alterWMTriggerDesc;
  private DropWMTriggerDesc dropWMTriggerDesc;

  private CreateOrAlterWMPoolDesc wmPoolDesc;
  private DropWMPoolDesc dropWMPoolDesc;

  private CreateOrAlterWMMappingDesc wmMappingDesc;
  private DropWMMappingDesc dropWMMappingDesc;

  private CreateOrDropTriggerToPoolMappingDesc triggerToPoolMappingDesc;

  private ReplRemoveFirstIncLoadPendFlagDesc replSetFirstIncLoadFlagDesc;

  boolean needLock = false;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected HashSet<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected HashSet<WriteEntity> outputs;
  private AlterTablePartMergeFilesDesc mergeFilesDesc;
  private CacheMetadataDesc cacheMetadataDesc;

  public DDLWork() {
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowConfDesc showConfDesc) {
    this(inputs, outputs);
    this.showConfDesc = showConfDesc;
  }

  /**
   * @param alterTblDesc
   *          alter table descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTableDesc alterTblDesc) {
    this(inputs, outputs);
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @param alterMVDesc
   *          alter materialized view descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterMaterializedViewDesc alterMVDesc) {
    this(inputs, outputs);
    this.alterMVDesc = alterMVDesc;
  }

  /**
   * @param createVwDesc
   *          create view descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateViewDesc createVwDesc) {
    this(inputs, outputs);

    this.createVwDesc = createVwDesc;
  }

  /**
   * @param dropTblDesc
   *          drop table descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropPartitionDesc dropPartitionDesc) {
    this(inputs, outputs);

    this.dropPartitionDesc = dropPartitionDesc;
  }

  /**
   * @param showColumnsDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowColumnsDesc showColumnsDesc) {
    this(inputs, outputs);

    this.showColumnsDesc = showColumnsDesc;
  }

  /**
   * @param showFuncsDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowFunctionsDesc showFuncsDesc) {
    this(inputs, outputs);

    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @param showLocksDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowLocksDesc showLocksDesc) {
    this(inputs, outputs);

    this.showLocksDesc = showLocksDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
                 ShowCompactionsDesc showCompactionsDesc) {
    this(inputs, outputs);
    this.showCompactionsDesc = showCompactionsDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
                 ShowTxnsDesc showTxnsDesc) {
    this(inputs, outputs);
    this.showTxnsDesc = showTxnsDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
                 AbortTxnsDesc abortTxnsDesc) {
    this(inputs, outputs);
    this.abortTxnsDesc = abortTxnsDesc;
  }

   /**
   * @param descFuncDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DescFunctionDesc descFuncDesc) {
    this(inputs, outputs);

    descFunctionDesc = descFuncDesc;
  }

  /**
   * @param showPartsDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowPartitionsDesc showPartsDesc) {
    this(inputs, outputs);

    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @param addPartitionDesc
   *          information about the partitions we want to add.
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AddPartitionDesc addPartitionDesc) {
    this(inputs, outputs);

    this.addPartitionDesc = addPartitionDesc;
  }

  /**
   * @param renamePartitionDesc
   *          information about the partitions we want to add.
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      RenamePartitionDesc renamePartitionDesc) {
    this(inputs, outputs);

    this.renamePartitionDesc = renamePartitionDesc;
  }

  /**
   * @param inputs
   * @param outputs
   * @param simpleDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTableSimpleDesc simpleDesc) {
    this(inputs, outputs);

    this.alterTblSimpleDesc = simpleDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      MsckDesc checkDesc) {
    this(inputs, outputs);

    msckDesc = checkDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      RoleDDLDesc roleDDLDesc) {
    this(inputs, outputs);
    this.roleDDLDesc = roleDDLDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      GrantDesc grantDesc) {
    this(inputs, outputs);
    this.grantDesc = grantDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowGrantDesc showGrant) {
    this(inputs, outputs);
    this.showGrantDesc = showGrant;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      RevokeDesc revokeDesc) {
    this(inputs, outputs);
    this.revokeDesc = revokeDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      GrantRevokeRoleDDL grantRevokeRoleDDL) {
    this(inputs, outputs);
    this.grantRevokeRoleDDL = grantRevokeRoleDDL;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTablePartMergeFilesDesc mergeDesc) {
    this(inputs, outputs);
    this.mergeFilesDesc = mergeDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTableAlterPartDesc alterPartDesc) {
    this(inputs, outputs);
    this.alterTableAlterPartDesc = alterPartDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTableExchangePartition alterTableExchangePartition) {
    this(inputs, outputs);
    this.alterTableExchangePartition = alterTableExchangePartition;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CacheMetadataDesc cacheMetadataDesc) {
    this(inputs, outputs);
    this.cacheMetadataDesc = cacheMetadataDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
          InsertCommitHookDesc insertCommitHookDesc
  ) {
    this(inputs, outputs);
    this.insertCommitHookDesc = insertCommitHookDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
                 KillQueryDesc killQueryDesc) {
    this(inputs, outputs);
    this.killQueryDesc = killQueryDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateResourcePlanDesc createResourcePlanDesc) {
    this(inputs, outputs);
    this.createResourcePlanDesc = createResourcePlanDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowResourcePlanDesc showResourcePlanDesc) {
    this(inputs, outputs);
    this.showResourcePlanDesc = showResourcePlanDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropResourcePlanDesc dropResourcePlanDesc) {
    this(inputs, outputs);
    this.dropResourcePlanDesc = dropResourcePlanDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterResourcePlanDesc alterResourcePlanDesc) {
    this(inputs, outputs);
    this.alterResourcePlanDesc = alterResourcePlanDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateWMTriggerDesc createWMTriggerDesc) {
    this(inputs, outputs);
    this.createWMTriggerDesc = createWMTriggerDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterWMTriggerDesc alterWMTriggerDesc) {
    this(inputs, outputs);
    this.alterWMTriggerDesc = alterWMTriggerDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropWMTriggerDesc dropWMTriggerDesc) {
    this(inputs, outputs);
    this.dropWMTriggerDesc = dropWMTriggerDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateOrAlterWMPoolDesc wmPoolDesc) {
    this(inputs, outputs);
    this.wmPoolDesc = wmPoolDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropWMPoolDesc dropWMPoolDesc) {
    this(inputs, outputs);
    this.dropWMPoolDesc = dropWMPoolDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateOrAlterWMMappingDesc wmMappingDesc) {
    this(inputs, outputs);
    this.wmMappingDesc = wmMappingDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropWMMappingDesc dropWMMappingDesc) {
    this(inputs, outputs);
    this.dropWMMappingDesc = dropWMMappingDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateOrDropTriggerToPoolMappingDesc triggerToPoolMappingDesc) {
    this(inputs, outputs);
    this.triggerToPoolMappingDesc = triggerToPoolMappingDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
                 ReplRemoveFirstIncLoadPendFlagDesc replSetFirstIncLoadFlagDesc) {
    this(inputs, outputs);
    this.replSetFirstIncLoadFlagDesc = replSetFirstIncLoadFlagDesc;
  }

  /**
   * @return the createTblDesc
   */
  @Explain(displayName = "Create View Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public CreateViewDesc getCreateViewDesc() {
    return createVwDesc;
  }

  /**
   * @return the dropTblDesc
   */
  @Explain(displayName = "Drop Partition Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public DropPartitionDesc getDropPartitionDesc() {
    return dropPartitionDesc;
  }

  /**
   * @return the alterTblDesc
   */
  @Explain(displayName = "Alter Table Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public AlterTableDesc getAlterTblDesc() {
    return alterTblDesc;
  }


  /**
   * @return the alterMVDesc
   */
  @Explain(displayName = "Alter Materialized View Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public AlterMaterializedViewDesc getAlterMaterializedViewDesc() {
    return alterMVDesc;
  }

  /**
   * @return the showColumnsDesc
   */
  @Explain(displayName = "Show Columns Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ShowColumnsDesc getShowColumnsDesc() {
    return showColumnsDesc;
  }

  /**
   * @return the showFuncsDesc
   */
  @Explain(displayName = "Show Function Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ShowFunctionsDesc getShowFuncsDesc() {
    return showFuncsDesc;
  }

  /**
   * @return the showLocksDesc
   */
  @Explain(displayName = "Show Lock Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ShowLocksDesc getShowLocksDesc() {
    return showLocksDesc;
  }

  @Explain(displayName = "Show Compactions Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ShowCompactionsDesc getShowCompactionsDesc() {
    return showCompactionsDesc;
  }

  @Explain(displayName = "Show Transactions Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ShowTxnsDesc getShowTxnsDesc() {
    return showTxnsDesc;
  }

  @Explain(displayName = "Abort Transactions Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public AbortTxnsDesc getAbortTxnsDesc() {
    return abortTxnsDesc;
  }

  /**
   * @return the descFuncDesc
   */
  @Explain(displayName = "Show Function Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public DescFunctionDesc getDescFunctionDesc() {
    return descFunctionDesc;
  }

  @Explain(displayName = "Kill Query Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public KillQueryDesc getKillQueryDesc() {
    return killQueryDesc;
  }

  /**
   * @return the showPartsDesc
   */
  @Explain(displayName = "Show Partitions Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ShowPartitionsDesc getShowPartsDesc() {
    return showPartsDesc;
  }

  /**
   * @return information about the partitions we want to add.
   */
  @Explain(displayName = "Add Partition Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public AddPartitionDesc getAddPartitionDesc() {
    return addPartitionDesc;
  }

  /**
   * @return information about the partitions we want to rename.
   */
  public RenamePartitionDesc getRenamePartitionDesc() {
    return renamePartitionDesc;
  }

  /**
   * @return information about the table/partitions we want to alter.
   */
  public AlterTableSimpleDesc getAlterTblSimpleDesc() {
    return alterTblSimpleDesc;
  }

  /**
   * @return Metastore check description
   */
  public MsckDesc getMsckDesc() {
    return msckDesc;
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public HashSet<WriteEntity> getOutputs() {
    return outputs;
  }

  /**
   * @return role ddl desc
   */
  public RoleDDLDesc getRoleDDLDesc() {
    return roleDDLDesc;
  }

  /**
   * @return grant desc
   */
  public GrantDesc getGrantDesc() {
    return grantDesc;
  }

  /**
   * @return show grant desc
   */
  public ShowGrantDesc getShowGrantDesc() {
    return showGrantDesc;
  }

  public RevokeDesc getRevokeDesc() {
    return revokeDesc;
  }

  public GrantRevokeRoleDDL getGrantRevokeRoleDDL() {
    return grantRevokeRoleDDL;
  }

  /**
   * @return descriptor for merging files
   */
  public AlterTablePartMergeFilesDesc getMergeFilesDesc() {
    return mergeFilesDesc;
  }

  public boolean getNeedLock() {
    return needLock;
  }

  public void setNeedLock(boolean needLock) {
    this.needLock = needLock;
  }

  /**
   * @return information about the partitions we want to change.
   */
  public AlterTableAlterPartDesc getAlterTableAlterPartDesc() {
    return alterTableAlterPartDesc;
  }

  /**
   * @return information about the table partition to be exchanged
   */
  public AlterTableExchangePartition getAlterTableExchangePartition() {
    return this.alterTableExchangePartition;
  }

  /**
   * @return information about the metadata to be cached
   */
  public CacheMetadataDesc getCacheMetadataDesc() {
    return this.cacheMetadataDesc;
  }

  public ShowConfDesc getShowConfDesc() {
    return showConfDesc;
  }

  @Explain(displayName = "Insert operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public InsertCommitHookDesc getInsertCommitHookDesc() {
    return insertCommitHookDesc;
  }

  @Explain(displayName = "Create resource plan")
  public CreateResourcePlanDesc getCreateResourcePlanDesc() {
    return createResourcePlanDesc;
  }

  @Explain(displayName = "Show resource plan")
  public ShowResourcePlanDesc getShowResourcePlanDesc() {
    return showResourcePlanDesc;
  }

  public DropResourcePlanDesc getDropResourcePlanDesc() {
    return dropResourcePlanDesc;
  }

  public AlterResourcePlanDesc getAlterResourcePlanDesc() {
    return alterResourcePlanDesc;
  }

  public CreateWMTriggerDesc getCreateWMTriggerDesc() {
    return createWMTriggerDesc;
  }

  public AlterWMTriggerDesc getAlterWMTriggerDesc() {
    return alterWMTriggerDesc;
  }

  public DropWMTriggerDesc getDropWMTriggerDesc() {
    return dropWMTriggerDesc;
  }

  public CreateOrAlterWMPoolDesc getWmPoolDesc() {
    return wmPoolDesc;
  }

  public DropWMPoolDesc getDropWMPoolDesc() {
    return dropWMPoolDesc;
  }

  public CreateOrAlterWMMappingDesc getWmMappingDesc() {
    return wmMappingDesc;
  }

  public DropWMMappingDesc getDropWMMappingDesc() {
    return dropWMMappingDesc;
  }

  public CreateOrDropTriggerToPoolMappingDesc getTriggerToPoolMappingDesc() {
    return triggerToPoolMappingDesc;
  }

  public void setTriggerToPoolMappingDesc(CreateOrDropTriggerToPoolMappingDesc triggerToPoolMappingDesc) {
    this.triggerToPoolMappingDesc = triggerToPoolMappingDesc;
  }

  public ReplRemoveFirstIncLoadPendFlagDesc getReplSetFirstIncLoadFlagDesc() {
    return replSetFirstIncLoadFlagDesc;
  }

  public void setReplSetFirstIncLoadFlagDesc(ReplRemoveFirstIncLoadPendFlagDesc replSetFirstIncLoadFlagDesc) {
    this.replSetFirstIncLoadFlagDesc = replSetFirstIncLoadFlagDesc;
  }
}
