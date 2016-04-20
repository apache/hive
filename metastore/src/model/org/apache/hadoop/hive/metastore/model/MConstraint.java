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
    MTable childTable, MColumnDescriptor parentColumn,
    MColumnDescriptor childColumn) {
   this.constraintName = constraintName;
   this.constraintType = constraintType;
   this.parentColumn = parentColumn;
   this.parentTable = parentTable;
   this.childColumn = childColumn;
   this.childTable = childTable;
   this.position = position;
   this.deleteRule = deleteRule;
   this.updateRule = updateRule;
   this.enableValidateRely = enableRelyValidate;
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

  public MColumnDescriptor getChildColumn() {
    return childColumn;
  }

  public void setChildColumn(MColumnDescriptor cc) {
    this.childColumn = cc;
  }

  public MColumnDescriptor getParentColumn() {
    return parentColumn;
  }

  public void setParentColumn(MColumnDescriptor pc) {
    this.parentColumn = pc;
  }
}
