/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
