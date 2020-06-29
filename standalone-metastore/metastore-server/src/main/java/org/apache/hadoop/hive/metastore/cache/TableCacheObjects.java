package org.apache.hadoop.hive.metastore.cache;

import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;

import java.util.List;

// Holder class for table objects. Currently this holds only constraints.
// ToDo: Add partitions/column stats
public class TableCacheObjects {
  private List<SQLPrimaryKey> primaryKeys;
  private List<SQLForeignKey> foreignKeys;
  private List<SQLNotNullConstraint> notNullConstraints;
  private List<SQLUniqueConstraint> uniqueConstraints;

  public List<SQLPrimaryKey> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(List<SQLPrimaryKey> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public List<SQLForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(List<SQLForeignKey> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  public List<SQLNotNullConstraint> getNotNullConstraints() {
    return notNullConstraints;
  }

  public void setNotNullConstraints(List<SQLNotNullConstraint> notNullConstraints) {
    this.notNullConstraints = notNullConstraints;
  }

  public List<SQLUniqueConstraint> getUniqueConstraints() {
    return uniqueConstraints;
  }

  public void setUniqueConstraints(List<SQLUniqueConstraint> uniqueConstraints) {
    this.uniqueConstraints = uniqueConstraints;
  }
}
