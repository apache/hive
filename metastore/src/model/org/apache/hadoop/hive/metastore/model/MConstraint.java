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
package org.apache.hadoop.hive.metastore.model;

import java.io.Serializable;

public class MConstraint
{
  String constraintName;
  int constraintType;
  int position;
  Integer deleteRule;
  Integer updateRule;
  MTable parentTable;
  MTable childTable;
  MColumnDescriptor parentColumn;
  MColumnDescriptor childColumn;
  Integer childIntegerIndex;
  Integer parentIntegerIndex;
  int enableValidateRely;

  // 0 - Primary Key
  // 1 - PK-FK relationship
  public final static int PRIMARY_KEY_CONSTRAINT = 0;
  public final static int FOREIGN_KEY_CONSTRAINT = 1;

  @SuppressWarnings("serial")
  public static class PK implements Serializable {
    public String constraintName;
    public int position;

    public PK() {}

    public PK(String constraintName, int position) {
      this.constraintName = constraintName;
      this.position = position;
    }

    public String toString() {
      return constraintName+":"+position;
    }

    public int hashCode() {
      return toString().hashCode();
    }

    public boolean equals(Object other) {
      if (other != null && (other instanceof PK)) {
        PK otherPK = (PK) other;
        return otherPK.constraintName.equals(constraintName) && otherPK.position == position;
      }
      return false;
    }
  }

  public MConstraint() {}

  public MConstraint(String constraintName, int constraintType, int position, Integer deleteRule, Integer updateRule, int enableRelyValidate, MTable parentTable,
    MTable childTable, MColumnDescriptor parentColumn, MColumnDescriptor childColumn, Integer childIntegerIndex, Integer parentIntegerIndex) {
   this.constraintName = constraintName;
   this.constraintType = constraintType;
   this.parentTable = parentTable;
   this.childTable = childTable;
   this.parentColumn = parentColumn;
   this.childColumn = childColumn;
   this.position = position;
   this.deleteRule = deleteRule;
   this.updateRule = updateRule;
   this.enableValidateRely = enableRelyValidate;
   this.childIntegerIndex = childIntegerIndex;
   this.parentIntegerIndex = parentIntegerIndex;
  }

  public String getConstraintName() {
    return constraintName;
  }

  public void setConstraintName(String fkName) {
    this.constraintName = fkName;
  }

  public int getConstraintType() {
    return constraintType;
  }

  public void setConstraintType(int ct) {
    this.constraintType = ct;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int po) {
    this.position = po;
  }

  public Integer getDeleteRule() {
    return deleteRule;
  }

  public void setDeleteRule(Integer de) {
    this.deleteRule = de;
  }

  public int getEnableValidateRely() {
    return enableValidateRely;
  }

  public void setEnableValidateRely(int enableValidateRely) {
    this.enableValidateRely = enableValidateRely;
  }

  public Integer getChildIntegerIndex() {
    return childIntegerIndex;
  }

  public void setChildIntegerIndex(Integer childIntegerIndex) {
    this.childIntegerIndex = childIntegerIndex;
  }

  public Integer getParentIntegerIndex() {
    return childIntegerIndex;
  }

  public void setParentIntegerIndex(Integer parentIntegerIndex) {
    this.parentIntegerIndex = parentIntegerIndex;
  }

  public Integer getUpdateRule() {
    return updateRule;
  }

  public void setUpdateRule(Integer ur) {
    this.updateRule = ur;
  }

  public MTable getChildTable() {
    return childTable;
  }

  public void setChildTable(MTable ft) {
    this.childTable = ft;
  }

  public MTable getParentTable() {
    return parentTable;
  }

  public void setParentTable(MTable pt) {
    this.parentTable = pt;
  }

  public MColumnDescriptor getParentColumn() {
    return parentColumn;
  }

  public void setParentColumn(MColumnDescriptor name) {
    this.parentColumn = name;
  }

  public MColumnDescriptor getChildColumn() {
    return childColumn;
  }

  public void setChildColumn(MColumnDescriptor name) {
    this.childColumn = name;
  }
}
