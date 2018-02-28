package org.apache.hadoop.hive.serde2.typeinfo;

public class MetastoreTypeInfoUtils {
  private MetastoreTypeInfoUtils() {
  }

  /**
   * Metastore is not supposed to enforce type ranges. The type range checks should be left
   * to the implementation engines. This method does a very lenient check which is obvious
   * and makes sense for overall sanity of decimal types
   * @param precision decimal precision value
   * @param scale decimal scale value
   */
  public static void validateDecimalParameters(int precision, int scale) {
    if (precision < 0) {
      throw new IllegalArgumentException("Precision cannot be negative");
    }
    if (scale < 0) {
      throw new IllegalArgumentException("Scale cannot be negative");
    }
  }

  /**
   * Metastore is not supposed to enforce type ranges. The type range checks should be left
   * to the implementation engines. This method does a very lenient check which is obvious
   * and makes sense for overall sanity of char types
   * @param length
   */
  public static void validateCharVarCharParameters(int length) {
    if (length < 0) {
      throw new IllegalArgumentException("Length cannot be negative");
    }
  }

  static String getQualifiedPrimitiveTypeName(String type, Object... parameters) {
    StringBuilder sb = new StringBuilder(type);
    if (parameters == null || parameters.length == 0) {
      return sb.toString();
    }
    sb.append('(');
    for (int i = 0; i < parameters.length; i++) {
      sb.append(parameters[i]);
      if (i != (parameters.length - 1)) {
        sb.append(',');
      }
    }
    sb.append(')');
    return sb.toString();
  }

  public static String getBaseName(String typeName) {
    int idx = typeName.indexOf('(');
    if (idx == -1) {
      return typeName;
    } else {
      return typeName.substring(0, idx);
    }
  }
}
