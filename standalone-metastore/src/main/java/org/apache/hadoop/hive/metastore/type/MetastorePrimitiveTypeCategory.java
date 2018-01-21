package org.apache.hadoop.hive.metastore.type;

import static org.apache.hadoop.hive.metastore.ColumnType.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.CHAR_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.DATETIME_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.INTERVAL_DAY_TIME_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.INTERVAL_YEAR_MONTH_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.INT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMPLOCALTZ_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMPTZ_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.VARCHAR_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.VOID_TYPE_NAME;

public enum MetastorePrimitiveTypeCategory {
  //this mapping is derived from PrimitiveObjectInspectorUtils in Hive
  //It maps the primitive category with the string representation of the type
  //Note that tinyint maps to byte, smallint maps to short and bigint maps to long below
  VOID(VOID_TYPE_NAME),
  BOOLEAN(BOOLEAN_TYPE_NAME),
  BYTE(TINYINT_TYPE_NAME),
  SHORT(SMALLINT_TYPE_NAME),
  INT(INT_TYPE_NAME),
  LONG(BIGINT_TYPE_NAME),
  FLOAT(FLOAT_TYPE_NAME),
  DOUBLE(DOUBLE_TYPE_NAME),
  STRING(STRING_TYPE_NAME),
  VARCHAR(VARCHAR_TYPE_NAME),
  CHAR(CHAR_TYPE_NAME),
  DATE(DATE_TYPE_NAME),
  DATETIME(DATETIME_TYPE_NAME),
  TIMESTAMP(TIMESTAMP_TYPE_NAME),
  INTERVAL_YEAR_MONTH(INTERVAL_YEAR_MONTH_TYPE_NAME),
  INTERVAL_DAY_TIME(INTERVAL_DAY_TIME_TYPE_NAME),
  DECIMAL(DECIMAL_TYPE_NAME),
  BINARY(BINARY_TYPE_NAME),
  TIMESTAMPTZ(TIMESTAMPTZ_TYPE_NAME),
  TIMESTAMPLOCALTZ(TIMESTAMPLOCALTZ_TYPE_NAME);

  private final String typeName;
  MetastorePrimitiveTypeCategory(String typeName) {
    this.typeName = typeName;
  }

  public String getTypeName() {
    return typeName;
  }

  public static MetastorePrimitiveTypeCategory from(String typeName) {
    for (MetastorePrimitiveTypeCategory primitiveTypeCategory : MetastorePrimitiveTypeCategory
        .values()) {
      if (primitiveTypeCategory.getTypeName().equals(typeName)) {
        return primitiveTypeCategory;
      }
    }
    return null;
  }
}
