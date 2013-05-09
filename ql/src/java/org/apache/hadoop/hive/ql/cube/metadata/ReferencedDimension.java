package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class ReferencedDimension extends BaseDimension {
  private final TableReference reference;

  public ReferencedDimension(FieldSchema column, TableReference reference) {
    super(column);
    this.reference = reference;
  }

  public TableReference getReference() {
    return reference;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimensionSrcReferenceKey(getName()),
        MetastoreUtil.getDimensionDestReference(reference));
  }

  public ReferencedDimension(String name, Map<String, String> props) {
    super(name, props);
    this.reference = new TableReference(
        props.get(MetastoreUtil.getDimensionSrcReferenceKey(getName())));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getReference() == null) ? 0 :
        getReference().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ReferencedDimension other = (ReferencedDimension) obj;
    if (this.getReference() == null) {
      if (other.getReference() != null) {
        return false;
      }
    } else if (!this.getReference().equals(other.getReference())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "reference:" + getReference();
    return str;
  }
}
