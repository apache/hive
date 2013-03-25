package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class BaseDimension extends CubeDimension {
  private final String type;

  public BaseDimension(FieldSchema column) {
    super(column.getName());
    this.type = column.getType();
    assert (type != null);
  }

  public String getType() {
    return type;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimTypePropertyKey(getName()), type);
  }

  public BaseDimension(String name, Map<String, String> props) {
    super(name);
    this.type = getDimType(name, props);
  }

  public static String getDimType(String name, Map<String, String> props) {
    return props.get(MetastoreUtil.getDimTypePropertyKey(name));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getType() == null) ? 0 :
      getType().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    BaseDimension other = (BaseDimension)obj;
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
