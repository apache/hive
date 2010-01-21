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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Set;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;

public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private createTableDesc createTblDesc;
  private createTableLikeDesc createTblLikeDesc;
  private createViewDesc createVwDesc;
  private dropTableDesc dropTblDesc;
  private alterTableDesc alterTblDesc;
  private showTablesDesc showTblsDesc;
  private showFunctionsDesc showFuncsDesc;
  private descFunctionDesc descFunctionDesc;
  private showPartitionsDesc showPartsDesc;
  private descTableDesc descTblDesc;
  private AddPartitionDesc addPartitionDesc;
  private MsckDesc msckDesc;
  private showTableStatusDesc showTblStatusDesc;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected Set<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected Set<WriteEntity> outputs;

  public DDLWork() {
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  /**
   * @param alterTblDesc
   *          alter table descriptor
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      alterTableDesc alterTblDesc) {
    this(inputs, outputs);
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @param createTblDesc
   *          create table descriptor
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      createTableDesc createTblDesc) {
    this(inputs, outputs);

    this.createTblDesc = createTblDesc;
  }

  /**
   * @param createTblLikeDesc
   *          create table like descriptor
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      createTableLikeDesc createTblLikeDesc) {
    this(inputs, outputs);

    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @param createVwDesc
   *          create view descriptor
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      createViewDesc createVwDesc) {
    this(inputs, outputs);

    this.createVwDesc = createVwDesc;
  }

  /**
   * @param dropTblDesc
   *          drop table descriptor
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      dropTableDesc dropTblDesc) {
    this(inputs, outputs);

    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @param descTblDesc
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      descTableDesc descTblDesc) {
    this(inputs, outputs);

    this.descTblDesc = descTblDesc;
  }

  /**
   * @param showTblsDesc
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      showTablesDesc showTblsDesc) {
    this(inputs, outputs);

    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @param showFuncsDesc
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      showFunctionsDesc showFuncsDesc) {
    this(inputs, outputs);

    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @param descFuncDesc
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      descFunctionDesc descFuncDesc) {
    this(inputs, outputs);

    descFunctionDesc = descFuncDesc;
  }

  /**
   * @param showPartsDesc
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      showPartitionsDesc showPartsDesc) {
    this(inputs, outputs);

    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @param addPartitionDesc
   *          information about the partitions we want to add.
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      AddPartitionDesc addPartitionDesc) {
    this(inputs, outputs);

    this.addPartitionDesc = addPartitionDesc;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      MsckDesc checkDesc) {
    this(inputs, outputs);

    msckDesc = checkDesc;
  }

  /**
   * @param showTblStatusDesc
   *          show table status descriptor
   */
  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      showTableStatusDesc showTblStatusDesc) {
    this(inputs, outputs);

    this.showTblStatusDesc = showTblStatusDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName = "Create Table Operator")
  public createTableDesc getCreateTblDesc() {
    return createTblDesc;
  }

  /**
   * @param createTblDesc
   *          the createTblDesc to set
   */
  public void setCreateTblDesc(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName = "Create Table Operator")
  public createTableLikeDesc getCreateTblLikeDesc() {
    return createTblLikeDesc;
  }

  /**
   * @param createTblLikeDesc
   *          the createTblDesc to set
   */
  public void setCreateTblLikeDesc(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName = "Create View Operator")
  public createViewDesc getCreateViewDesc() {
    return createVwDesc;
  }

  /**
   * @param createVwDesc
   *          the createViewDesc to set
   */
  public void setCreateViewDesc(createViewDesc createVwDesc) {
    this.createVwDesc = createVwDesc;
  }

  /**
   * @return the dropTblDesc
   */
  @explain(displayName = "Drop Table Operator")
  public dropTableDesc getDropTblDesc() {
    return dropTblDesc;
  }

  /**
   * @param dropTblDesc
   *          the dropTblDesc to set
   */
  public void setDropTblDesc(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @return the alterTblDesc
   */
  @explain(displayName = "Alter Table Operator")
  public alterTableDesc getAlterTblDesc() {
    return alterTblDesc;
  }

  /**
   * @param alterTblDesc
   *          the alterTblDesc to set
   */
  public void setAlterTblDesc(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @return the showTblsDesc
   */
  @explain(displayName = "Show Table Operator")
  public showTablesDesc getShowTblsDesc() {
    return showTblsDesc;
  }

  /**
   * @param showTblsDesc
   *          the showTblsDesc to set
   */
  public void setShowTblsDesc(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @return the showFuncsDesc
   */
  @explain(displayName = "Show Function Operator")
  public showFunctionsDesc getShowFuncsDesc() {
    return showFuncsDesc;
  }

  /**
   * @return the descFuncDesc
   */
  @explain(displayName = "Show Function Operator")
  public descFunctionDesc getDescFunctionDesc() {
    return descFunctionDesc;
  }

  /**
   * @param showFuncsDesc
   *          the showFuncsDesc to set
   */
  public void setShowFuncsDesc(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @param descFuncDesc
   *          the showFuncsDesc to set
   */
  public void setDescFuncDesc(descFunctionDesc descFuncDesc) {
    descFunctionDesc = descFuncDesc;
  }

  /**
   * @return the showPartsDesc
   */
  @explain(displayName = "Show Partitions Operator")
  public showPartitionsDesc getShowPartsDesc() {
    return showPartsDesc;
  }

  /**
   * @param showPartsDesc
   *          the showPartsDesc to set
   */
  public void setShowPartsDesc(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @return the descTblDesc
   */
  @explain(displayName = "Describe Table Operator")
  public descTableDesc getDescTblDesc() {
    return descTblDesc;
  }

  /**
   * @param descTblDesc
   *          the descTblDesc to set
   */
  public void setDescTblDesc(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  /**
   * @return information about the partitions we want to add.
   */
  public AddPartitionDesc getAddPartitionDesc() {
    return addPartitionDesc;
  }

  /**
   * @param addPartitionDesc
   *          information about the partitions we want to add.
   */
  public void setAddPartitionDesc(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  /**
   * @return Metastore check description
   */
  public MsckDesc getMsckDesc() {
    return msckDesc;
  }

  /**
   * @param msckDesc
   *          metastore check description
   */
  public void setMsckDesc(MsckDesc msckDesc) {
    this.msckDesc = msckDesc;
  }

  /**
   * @return show table descriptor
   */
  public showTableStatusDesc getShowTblStatusDesc() {
    return showTblStatusDesc;
  }

  /**
   * @param showTblStatusDesc
   *          show table descriptor
   */
  public void setShowTblStatusDesc(showTableStatusDesc showTblStatusDesc) {
    this.showTblStatusDesc = showTblStatusDesc;
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setInputs(Set<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(Set<WriteEntity> outputs) {
    this.outputs = outputs;
  }

}
