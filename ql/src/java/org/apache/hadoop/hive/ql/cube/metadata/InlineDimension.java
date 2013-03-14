package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class InlineDimension extends BaseDimension {

  private final List<String> values;

  public InlineDimension(FieldSchema column, List<String> values) {
    super(column);
    this.values = values;
  }

  public List<String> getValues() {
    return values;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getInlineDimensionSizeKey(getName()),
        String.valueOf(values.size()));
    props.put(MetastoreUtil.getInlineDimensionValuesKey(getName()),
        MetastoreUtil.getStr(values));
  }

  public InlineDimension(String name, Map<String, String> props) {
    super(name, props);
    String valueStr = props.get(MetastoreUtil.getInlineDimensionValuesKey(name));
    this.values = Arrays.asList(valueStr.split(","));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getValues() == null) ? 0 :
      getValues().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    InlineDimension other = (InlineDimension)obj;
    if (this.getValues() == null) {
      if (other.getValues() != null) {
        return false;
      }
    } else if (!this.getValues().equals(other.getValues())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "values:" + MetastoreUtil.getStr(values);
    return str;
  }

}
