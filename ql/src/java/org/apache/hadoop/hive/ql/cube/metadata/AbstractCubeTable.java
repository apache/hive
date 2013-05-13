package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

public abstract class AbstractCubeTable implements Named {
  private final String name;
  private final List<FieldSchema> columns;
  private final Map<String, String> properties = new HashMap<String, String>();
  private final double weight;

  protected AbstractCubeTable(String name, List<FieldSchema> columns,
      Map<String, String> props) {
    this(name, columns, props, 0L);
  }

  protected AbstractCubeTable(String name, List<FieldSchema> columns,
      Map<String, String> props, double weight) {
    this.name = name;
    this.columns = columns;
    this.weight = weight;
    this.properties.putAll(props);
  }

  protected AbstractCubeTable(Table hiveTable) {
    this.name = hiveTable.getTableName();
    this.columns = hiveTable.getCols();
    this.properties.putAll(hiveTable.getParameters());
    this.weight = getWeight(getProperties(), getName());
  }

  public abstract CubeTableType getTableType();

  public abstract Set<String> getStorages();

  public Map<String, String> getProperties() {
    return properties;
  }

  public static double getWeight(Map<String, String> properties, String name) {
    String wtStr = properties.get(MetastoreUtil.getCubeTableWeightKey(name));
     return wtStr == null ? 0L : Double.parseDouble(wtStr);
  }

  protected void addProperties() {
    properties.put(MetastoreConstants.TABLE_TYPE_KEY, getTableType().name());
    properties.put(MetastoreUtil.getCubeTableWeightKey(name),
        String.valueOf(weight));
  }

  public String getName() {
    return name;
  }

  public List<FieldSchema> getColumns() {
    return columns;
  }

  public double weight() {
    return weight;
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
    AbstractCubeTable other = (AbstractCubeTable) obj;

    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (this.getColumns() == null) {
      if (other.getColumns() != null) {
        return false;
      }
    } else {
      if (!this.getColumns().equals(other.getColumns())) {
        return false;
      }
    }
    if (this.weight() != other.weight()) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getName();
  }
}
