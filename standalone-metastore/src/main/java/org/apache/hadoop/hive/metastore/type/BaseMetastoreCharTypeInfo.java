package org.apache.hadoop.hive.metastore.type;

public abstract class BaseMetastoreCharTypeInfo extends PrimitiveMetastoreTypeInfo {
  private static final long serialVersionUID = 1L;

  private int length;

  // no-arg constructor to make kyro happy.
  public BaseMetastoreCharTypeInfo() {
  }

  public BaseMetastoreCharTypeInfo(String typeName) {
    super(typeName);
  }

  public BaseMetastoreCharTypeInfo(String typeName, int length) {
    super(typeName);
    this.length = length;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  @Override
  public String getQualifiedName() {
    return getQualifiedName(typeName, length);
  }

  public static String getQualifiedName(String typeName, int length) {
    StringBuilder sb = new StringBuilder(typeName);
    sb.append("(");
    sb.append(length);
    sb.append(")");
    return sb.toString();
  }
}
