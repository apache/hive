package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class BaseDimension {
  private final FieldSchema column;

  public BaseDimension(FieldSchema column) {
    this.column = column;
    assert (column != null);
    assert (column.getName() != null);
    assert (column.getType() != null);
  }

  public FieldSchema getColumn() {
    return column;
  }

  public String getName() {
    return column.getName();
  }

  public String getType() {
    return column.getType();
  }

  public void addProperties(Map<String, String> props) {
    props.put(MetastoreUtil.getDimTypePropertyKey(column.getName()),
        column.getType());
  }

  public BaseDimension(String name, Map<String, String> props) {
    String type = getDimType(name, props);
    this.column = new FieldSchema(name, type, "");
  }

  public static String getDimType(String name, Map<String, String> props) {
    return props.get(MetastoreUtil.getDimTypePropertyKey(name));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 :
      getName().toLowerCase().hashCode());
    result = prime * result + ((getType() == null) ? 0 :
      getType().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    BaseDimension other = (BaseDimension)obj;
    if (this.getName() == null) {
      if (other.getName() != null) {
        return false;
      }
    } else if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (this.getType() == null) {
      if (other.getType() != null) {
        return false;
      }
    } else if (!this.getType().equalsIgnoreCase(other.getType())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = getName() + ":" + getType();
    return str;
  }
}
