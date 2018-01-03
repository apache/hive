package org.apache.hadoop.hive.serde2.avro;

/**
 * This class contains some of the constants which are specific to AvroSerDe
 * They should always match with the constants defined in AvroSerDe.java in Hive Source code. These
 * constants were copied as part of separating metastore from Hive.
 */
public class AvroSerDeConstants {
  public static final String TABLE_NAME = "name";
  public static final String TABLE_COMMENT = "comment";
  public static final String LIST_COLUMN_COMMENTS = "columns.comments";

  //it just so happens that the AVRO has these constants which are same as defined in ColumnType
  //We should still keep it separate in case in future we need to separate the two
  public static final String DECIMAL_TYPE_NAME = "decimal";
  public static final String CHAR_TYPE_NAME = "char";
  public static final String VARCHAR_TYPE_NAME = "varchar";
  public static final String DATE_TYPE_NAME = "date";

  public static final String AVRO_TIMESTAMP_TYPE_NAME = "timestamp-millis";
  public static final String AVRO_PROP_LOGICAL_TYPE = "logicalType";
  public static final String AVRO_PROP_PRECISION = "precision";
  public static final String AVRO_PROP_SCALE = "scale";
  public static final String AVRO_PROP_MAX_LENGTH = "maxLength";
  public static final String AVRO_STRING_TYPE_NAME = "string";
  public static final String AVRO_INT_TYPE_NAME = "int";
  public static final String AVRO_LONG_TYPE_NAME = "long";
}
