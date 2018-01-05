package org.apache.hadoop.hive.metastore.schema.utils;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoParser;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

public class StorageSchemaUtils {

  public static final PrimitiveTypeEntry binaryTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.BINARY,
          serdeConstants.BINARY_TYPE_NAME, byte[].class, byte[].class, BytesWritable.class);
  public static final PrimitiveTypeEntry stringTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.STRING,
          serdeConstants.STRING_TYPE_NAME, null, String.class, Text.class);
  public static final PrimitiveTypeEntry booleanTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN,
          serdeConstants.BOOLEAN_TYPE_NAME, Boolean.TYPE, Boolean.class, BooleanWritable.class);
  public static final PrimitiveTypeEntry intTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.INT,
          serdeConstants.INT_TYPE_NAME, Integer.TYPE, Integer.class, IntWritable.class);
  public static final PrimitiveTypeEntry longTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.LONG,
          serdeConstants.BIGINT_TYPE_NAME, Long.TYPE, Long.class, LongWritable.class);
  public static final PrimitiveTypeEntry floatTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.FLOAT,
          serdeConstants.FLOAT_TYPE_NAME, Float.TYPE, Float.class, FloatWritable.class);
  public static final PrimitiveTypeEntry voidTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.VOID,
          serdeConstants.VOID_TYPE_NAME, Void.TYPE, Void.class, NullWritable.class);

  public static final PrimitiveTypeEntry doubleTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE,
          serdeConstants.DOUBLE_TYPE_NAME, Double.TYPE, Double.class, DoubleWritable.class);
  public static final PrimitiveTypeEntry byteTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.BYTE,
          serdeConstants.TINYINT_TYPE_NAME, Byte.TYPE, Byte.class, ByteWritable.class);
  public static final PrimitiveTypeEntry shortTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.SHORT,
          serdeConstants.SMALLINT_TYPE_NAME, Short.TYPE, Short.class, ShortWritable.class);
  public static final PrimitiveTypeEntry dateTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.DATE,
          serdeConstants.DATE_TYPE_NAME, null, Date.class, DateWritable.class);

  public static final PrimitiveTypeEntry timestampTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP,
          //serdeConstants.TIMESTAMP_TYPE_NAME, null, Timestamp.class, TimestampWritable.class);
          //TODO need to move TimestampWritable to storage-api to make this work
          serdeConstants.TIMESTAMP_TYPE_NAME, null, Timestamp.class, null);
  public static final PrimitiveTypeEntry timestampTZTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ,
          //serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, null, TimestampTZ.class,
          //TimestampLocalTZWritable.class);
          //TODO need to move TimestampTZ and TimestampLocalTZWritable to storage-api to make this work
          serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, null, null,
          null);
  public static final PrimitiveTypeEntry intervalYearMonthTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_YEAR_MONTH,
          //serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, null, HiveIntervalYearMonth.class,
          //HiveIntervalYearMonthWritable.class);
          //TODO need to move HiveIntervalYearMonth and HiveIntervalYearMonthWritable to storage-api to make this work
          serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, null, null,
          null);
  public static final PrimitiveTypeEntry intervalDayTimeTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_DAY_TIME,
          serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, null, HiveIntervalDayTime.class,
          //HiveIntervalDayTimeWritable.class);
          //TODO need to move HiveIntervalYearMonth and HiveIntervalDayTimeWritable to storage-api to make this work
          null);
  public static final PrimitiveTypeEntry decimalTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL,
          serdeConstants.DECIMAL_TYPE_NAME, null, HiveDecimal.class, HiveDecimalWritable.class);
  public static final PrimitiveTypeEntry varcharTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.VARCHAR,
          serdeConstants.VARCHAR_TYPE_NAME, null, HiveVarchar.class, HiveVarcharWritable.class);
  public static final PrimitiveTypeEntry charTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.CHAR,
          serdeConstants.CHAR_TYPE_NAME, null, HiveChar.class, HiveCharWritable.class);

  // The following is a complex type for special handling
  public static final PrimitiveTypeEntry unknownTypeEntry =
      new PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN, "unknown", null,
          Object.class, null);


  static {
    PrimitiveTypeEntry.registerType(binaryTypeEntry);
    PrimitiveTypeEntry.registerType(stringTypeEntry);
    PrimitiveTypeEntry.registerType(charTypeEntry);
    PrimitiveTypeEntry.registerType(varcharTypeEntry);
    PrimitiveTypeEntry.registerType(booleanTypeEntry);
    PrimitiveTypeEntry.registerType(intTypeEntry);
    PrimitiveTypeEntry.registerType(longTypeEntry);
    PrimitiveTypeEntry.registerType(floatTypeEntry);
    PrimitiveTypeEntry.registerType(voidTypeEntry);
    PrimitiveTypeEntry.registerType(doubleTypeEntry);
    PrimitiveTypeEntry.registerType(byteTypeEntry);
    PrimitiveTypeEntry.registerType(shortTypeEntry);
    PrimitiveTypeEntry.registerType(dateTypeEntry);
    PrimitiveTypeEntry.registerType(timestampTypeEntry);
    PrimitiveTypeEntry.registerType(timestampTZTypeEntry);
    PrimitiveTypeEntry.registerType(intervalYearMonthTypeEntry);
    PrimitiveTypeEntry.registerType(intervalDayTimeTypeEntry);
    PrimitiveTypeEntry.registerType(decimalTypeEntry);
    PrimitiveTypeEntry.registerType(unknownTypeEntry);
  }


  public static final char COMMA = ',';
  public static List<TypeInfo> getTypeInfosFromTypeString(String columnTypeProperty) {
    return new TypeInfoParser(columnTypeProperty).parseTypeInfos();
  }

  private static final String FROM_STORAGE_SCHEMA_READER = "generated by storage schema reader";
  public static String determineFieldComment(String comment) {
    return (comment == null) ? FROM_STORAGE_SCHEMA_READER : comment;
  }
}
