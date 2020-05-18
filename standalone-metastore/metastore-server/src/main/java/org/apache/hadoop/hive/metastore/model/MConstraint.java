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
package org.apache.hadoop.hive.metastore.model;

import java.io.Serializable;

/** Representing a row in the KEY_CONSTARINTS table. */
public class MConstraint
{
  private String constraintName; // Primary key of KEY_CONSTARINTS
  private int position; // Primary key of KEY_CONSTARINTS
  private int constraintType;
  private Integer deleteRule;
  private Integer updateRule;
  private MTable parentTable; // Primary key of KEY_CONSTARINTS
  private MTable childTable;
  private MColumnDescriptor parentColumn;
  private MColumnDescriptor childColumn;
  private Integer childIntegerIndex;
  private Integer parentIntegerIndex;
  private int enableValidateRely;
  private String defaultValue;

  // 0 - Primary Key
  // 1 - PK-FK relationship
  // 2 - Unique Constraint
  // 3 - Not Null Constraint
  public final static int PRIMARY_KEY_CONSTRAINT = 0;
  public final static int FOREIGN_KEY_CONSTRAINT = 1;
  public final static int UNIQUE_CONSTRAINT = 2;
  public final static int NOT_NULL_CONSTRAINT = 3;
  public final static int DEFAULT_CONSTRAINT = 4;
  public final static int CHECK_CONSTRAINT = 5;

  @SuppressWarnings("serial")
  public static class PK implements Serializable {
    public MTable.PK parentTable;
    public String constraintName;
    public int position;

    public PK() {}

    public PK(MTable.PK parentTable, String constraintName, int position) {
      this.parentTable = parentTable;
      this.constraintName = constraintName;
      this.position = position;
    }

    public String toString() {
      return String.format("%s:%s:%d", parentTable.id, constraintName, position);
    }

    public int hashCode() {
      return toString().hashCode();
    }

    public boolean equals(Object other) {
      if (other != null && (other instanceof PK)) {
        PK otherPK = (PK) other;
        return
            otherPK.parentTable.id == parentTable.id &&
            otherPK.constraintName.equals(constraintName) &&
            otherPK.position == position;
      }
      return false;
    }
  }

  public MConstraint() {}

  public MConstraint(String constraintName, int position, int constraintType, Integer deleteRule, Integer updateRule,
      int enableRelyValidate, MTable parentTable, MTable childTable, MColumnDescriptor parentColumn,
      MColumnDescriptor childColumn, Integer childIntegerIndex, Integer parentIntegerIndex) {
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

  public MConstraint(String constraintName, int position, int constraintType, Integer deleteRule, Integer updateRule,
      int enableRelyValidate, MTable parentTable, MTable childTable, MColumnDescriptor parentColumn,
      MColumnDescriptor childColumn, Integer childIntegerIndex, Integer parentIntegerIndex, String defaultValue) {
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
    this.defaultValue = defaultValue;
  }

  public String getConstraintName() {
    return constraintName;
  }

  public void setConstraintName(String constraintName) {
    this.constraintName = constraintName;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public int getConstraintType() {
    return constraintType;
  }

  public void setConstraintType(int constraintType) {
    this.constraintType = constraintType;
  }

  public Integer getDeleteRule() {
    return deleteRule;
  }

  public void setDeleteRule(Integer deleteRule) {
    this.deleteRule = deleteRule;
  }

  public Integer getUpdateRule() {
    return updateRule;
  }

  public void setUpdateRule(Integer updateRule) {
    this.updateRule = updateRule;
  }

  public MTable getParentTable() {
    return parentTable;
  }

  public void setParentTable(MTable parentTable) {
    this.parentTable = parentTable;
  }

  public MTable getChildTable() {
    return childTable;
  }

  public void setChildTable(MTable childTable) {
    this.childTable = childTable;
  }

  public MColumnDescriptor getParentColumn() {
    return parentColumn;
  }

  public void setParentColumn(MColumnDescriptor parentColumn) {
    this.parentColumn = parentColumn;
  }

  public MColumnDescriptor getChildColumn() {
    return childColumn;
  }

  public void setChildColumn(MColumnDescriptor childColumn) {
    this.childColumn = childColumn;
  }

  public Integer getChildIntegerIndex() {
    return childIntegerIndex;
  }

  public void setChildIntegerIndex(Integer childIntegerIndex) {
    this.childIntegerIndex = childIntegerIndex;
  }

  public Integer getParentIntegerIndex() {
    return parentIntegerIndex;
  }

  public void setParentIntegerIndex(Integer parentIntegerIndex) {
    this.parentIntegerIndex = parentIntegerIndex;
  }

  public int getEnableValidateRely() {
    return enableValidateRely;
  }

  public void setEnableValidateRely(int enableValidateRely) {
    this.enableValidateRely = enableValidateRely;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }
}
