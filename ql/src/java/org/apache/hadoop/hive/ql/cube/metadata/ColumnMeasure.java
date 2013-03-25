package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public final class ColumnMeasure extends CubeMeasure {
  public ColumnMeasure(FieldSchema column, String formatString,
      String aggregate, String unit) {
    super(column, formatString, aggregate, unit);
  }

  public ColumnMeasure(FieldSchema column) {
    this(column, null, null, null);
  }

  public ColumnMeasure(String name, Map<String, String> props) {
    super(name, props);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return true;
  }

}
