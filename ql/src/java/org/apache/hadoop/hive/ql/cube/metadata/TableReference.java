package org.apache.hadoop.hive.ql.cube.metadata;

public class TableReference {
  private String destTable;
  private String destColumn;

  public TableReference() {
  }

  public TableReference(String destTable, String destColumn) {
    this.destTable = destTable;
    this.destColumn = destColumn;
  }

  public TableReference(String reference) {
    String desttoks[] = reference.split("\\.+");
    this.destTable = desttoks[0];
    this.destColumn = desttoks[1];
  }

  public String getDestTable() {
    return destTable;
  }

  public void setDestTable(String dest) {
    this.destTable = dest;
  }

  public String getDestColumn() {
    return destColumn;
  }

  public void setDestColumn(String destColumn) {
    this.destColumn = destColumn;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableReference other = (TableReference) obj;
    if (this.getDestColumn() == null) {
      if (other.getDestColumn() != null) {
        return false;
      }
    } else if (!this.getDestColumn().equals(other.getDestColumn())) {
      return false;
    }
    if (this.getDestTable() == null) {
      if (other.getDestTable() != null) {
        return false;
      }
    } else if (!this.getDestTable().equals(other.getDestTable())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return destTable + "." + destColumn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((destColumn == null) ? 0 :
        destColumn.hashCode());
    result = prime * result + ((destTable == null) ? 0 : destTable.hashCode());
    return result;
  }
}
