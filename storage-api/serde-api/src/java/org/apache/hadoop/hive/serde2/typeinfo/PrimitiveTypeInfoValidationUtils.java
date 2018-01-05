package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;

public class PrimitiveTypeInfoValidationUtils {
  public static void validateVarcharParameter(int length) {
    if (length > HiveVarchar.MAX_VARCHAR_LENGTH || length < 1) {
      throw new RuntimeException("Varchar length " + length + " out of allowed range [1, " +
          HiveVarchar.MAX_VARCHAR_LENGTH + "]");
    }
  }

  public static void validateCharParameter(int length) {
    if (length > HiveChar.MAX_CHAR_LENGTH || length < 1) {
      throw new RuntimeException("Char length " + length + " out of allowed range [1, " +
          HiveChar.MAX_CHAR_LENGTH + "]");
    }
  }

  public static void validateParameter(int precision, int scale) {
    if (precision < 1 || precision > HiveDecimal.MAX_PRECISION) {
      throw new IllegalArgumentException("Decimal precision out of allowed range [1," +
          HiveDecimal.MAX_PRECISION + "]");
    }

    if (scale < 0 || scale > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Decimal scale out of allowed range [0," +
          HiveDecimal.MAX_SCALE + "]");
    }

    if (precision < scale) {
      throw new IllegalArgumentException("Decimal scale must be less than or equal to precision");
    }
  }
}