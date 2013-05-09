package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Map;

public abstract class CubeDimension implements Named {
  private final String name;

  public CubeDimension(String name) {
    this.name = name;
    assert (name != null);
  }

  public String getName() {
    return name;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 :
        getName().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    CubeDimension other = (CubeDimension) obj;
    if (this.getName() == null) {
      if (other.getName() != null) {
        return false;
      }
    } else if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    return true;
  }

  public void addProperties(Map<String, String> props) {
    props.put(MetastoreUtil.getDimensionClassPropertyKey(getName()),
        getClass().getName());
  }

  @Override
  public String toString() {
    return name;
  }
}
