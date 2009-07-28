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

public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private createTableDesc      createTblDesc;
  private createTableLikeDesc  createTblLikeDesc;
  private dropTableDesc        dropTblDesc;
  private alterTableDesc       alterTblDesc;
  private showTablesDesc       showTblsDesc;
  private showFunctionsDesc    showFuncsDesc;
  private showPartitionsDesc   showPartsDesc;
  private descTableDesc        descTblDesc;
  private AddPartitionDesc addPartitionDesc;
  private MsckDesc msckDesc;

  public DDLWork() { }

  /**
   * @param alterTblDesc alter table descriptor
   */
  public DDLWork(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @param createTblDesc create table descriptor
   */
  public DDLWork(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  /**
   * @param createTblLikeDesc create table dlike escriptor
   */
  public DDLWork(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @param dropTblDesc drop table descriptor
   */
  public DDLWork(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @param descTblDesc
   */
  public DDLWork(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  /**
   * @param showTblsDesc
   */
  public DDLWork(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @param showFuncsDesc
   */
  public DDLWork(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @param showPartsDesc
   */
  public DDLWork(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }
  
  /**
   * @param addPartitionDesc information about the partitions
   * we want to add.
   */
  public DDLWork(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  public DDLWork(MsckDesc checkDesc) {
    this.msckDesc = checkDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName="Create Table Operator")
  public createTableDesc getCreateTblDesc() {
    return createTblDesc;
  }

  /**
   * @param createTblDesc the createTblDesc to set
   */
  public void setCreateTblDesc(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName="Create Table Operator")
  public createTableLikeDesc getCreateTblLikeDesc() {
    return createTblLikeDesc;
  }

  /**
   * @param createTblDesc the createTblDesc to set
   */
  public void setCreateTblLikeDesc(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @return the dropTblDesc
   */
  @explain(displayName="Drop Table Operator")
  public dropTableDesc getDropTblDesc() {
    return dropTblDesc;
  }

  /**
   * @param dropTblDesc the dropTblDesc to set
   */
  public void setDropTblDesc(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @return the alterTblDesc
   */
  @explain(displayName="Alter Table Operator")
  public alterTableDesc getAlterTblDesc() {
    return alterTblDesc;
  }

  /**
   * @param alterTblDesc the alterTblDesc to set
   */
  public void setAlterTblDesc(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @return the showTblsDesc
   */
  @explain(displayName="Show Table Operator")
  public showTablesDesc getShowTblsDesc() {
    return showTblsDesc;
  }

  /**
   * @param showTblsDesc the showTblsDesc to set
   */
  public void setShowTblsDesc(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @return the showFuncsDesc
   */
  @explain(displayName="Show Function Operator")
  public showFunctionsDesc getShowFuncsDesc() {
    return showFuncsDesc;
  }

  /**
   * @param showFuncsDesc the showFuncsDesc to set
   */
  public void setShowFuncsDesc(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @return the showPartsDesc
   */
  @explain(displayName="Show Partitions Operator")
  public showPartitionsDesc getShowPartsDesc() {
    return showPartsDesc;
  }

  /**
   * @param showPartsDesc the showPartsDesc to set
   */
  public void setShowPartsDesc(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @return the descTblDesc
   */
  @explain(displayName="Describe Table Operator")
  public descTableDesc getDescTblDesc() {
    return descTblDesc;
  }

  /**
   * @param descTblDesc the descTblDesc to set
   */
  public void setDescTblDesc(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  /**
   * @return information about the partitions
   * we want to add.
   */
  public AddPartitionDesc getAddPartitionDesc() {
    return addPartitionDesc;
  }

  /**
   * @param addPartitionDesc information about the partitions
   * we want to add.
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
   * @param msckDesc metastore check description
   */
  public void setMsckDesc(MsckDesc msckDesc) {
    this.msckDesc = msckDesc;
  }
  
}
