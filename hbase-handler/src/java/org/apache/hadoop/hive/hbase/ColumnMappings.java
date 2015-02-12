/**
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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.collect.Iterators;

public class ColumnMappings implements Iterable<ColumnMappings.ColumnMapping> {

  private final int keyIndex;
  private final int timestampIndex;
  private final ColumnMapping[] columnsMapping;

  public ColumnMappings(List<ColumnMapping> columnMapping, int keyIndex) {
    this(columnMapping, keyIndex, -1);
  }

  public ColumnMappings(List<ColumnMapping> columnMapping, int keyIndex, int timestampIndex) {
    this.columnsMapping = columnMapping.toArray(new ColumnMapping[columnMapping.size()]);
    this.keyIndex = keyIndex;
    this.timestampIndex = timestampIndex;
  }

  @Override
  public Iterator<ColumnMapping> iterator() {
    return Iterators.forArray(columnsMapping);
  }

  public int size() {
    return columnsMapping.length;
  }

  String toNamesString(Properties tbl, String autogenerate) {
    if (autogenerate != null && autogenerate.equals("true")) {
      StringBuilder sb = new StringBuilder();
      HBaseSerDeHelper.generateColumns(tbl, Arrays.asList(columnsMapping), sb);
      return sb.toString();
    }

    return StringUtils.EMPTY; // return empty string
  }

  String toTypesString(Properties tbl, Configuration conf, String autogenerate)
      throws SerDeException {
    StringBuilder sb = new StringBuilder();

    if (autogenerate != null && autogenerate.equals("true")) {
      HBaseSerDeHelper.generateColumnTypes(tbl, Arrays.asList(columnsMapping), sb, conf);
    } else {
      for (ColumnMapping colMap : columnsMapping) {
        if (sb.length() > 0) {
          sb.append(":");
        }
        if (colMap.hbaseRowKey) {
          // the row key column becomes a STRING
          sb.append(serdeConstants.STRING_TYPE_NAME);
        } else if (colMap.qualifierName == null) {
          // a column family become a MAP
          sb.append(serdeConstants.MAP_TYPE_NAME + "<" + serdeConstants.STRING_TYPE_NAME + ","
              + serdeConstants.STRING_TYPE_NAME + ">");
        } else {
          // an individual column becomes a STRING
          sb.append(serdeConstants.STRING_TYPE_NAME);
        }
      }
    }

    return sb.toString();
  }

  void setHiveColumnDescription(String serdeName,
      List<String> columnNames, List<TypeInfo> columnTypes) throws SerDeException {
    if (columnsMapping.length != columnNames.size()) {
      throw new SerDeException(serdeName + ": columns has " + columnNames.size() +
          " elements while hbase.columns.mapping has " + columnsMapping.length + " elements" +
          " (counting the key if implicit)");
    }

    // check that the mapping schema is right;
    // check that the "column-family:" is mapped to  Map<key,?>
    // where key extends LazyPrimitive<?, ?> and thus has type Category.PRIMITIVE
    for (int i = 0; i < columnNames.size(); i++) {
      ColumnMapping colMap = columnsMapping[i];
      colMap.columnName = columnNames.get(i);
      colMap.columnType = columnTypes.get(i);
      if (colMap.qualifierName == null && !colMap.hbaseRowKey && !colMap.hbaseTimestamp) {
        TypeInfo typeInfo = columnTypes.get(i);
        if ((typeInfo.getCategory() != ObjectInspector.Category.MAP) ||
            (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getCategory()
                != ObjectInspector.Category.PRIMITIVE)) {

          throw new SerDeException(
              serdeName + ": hbase column family '" + colMap.familyName
                  + "' should be mapped to Map<? extends LazyPrimitive<?, ?>,?>, that is "
                  + "the Key for the map should be of primitive type, but is mapped to "
                  + typeInfo.getTypeName());
        }
      }
      if (colMap.hbaseTimestamp) {
        TypeInfo typeInfo = columnTypes.get(i);
        if (!colMap.isCategory(PrimitiveCategory.TIMESTAMP) &&
            !colMap.isCategory(PrimitiveCategory.LONG)) {
          throw new SerDeException(serdeName + ": timestamp columns should be of " +
              "timestamp or bigint type, but is mapped to " + typeInfo.getTypeName());
        }
      }
    }
  }

  /**
   * Utility method for parsing a string of the form '-,b,s,-,s:b,...' as a means of specifying
   * whether to use a binary or an UTF string format to serialize and de-serialize primitive
   * data types like boolean, byte, short, int, long, float, and double. This applies to
   * regular columns and also to map column types which are associated with an HBase column
   * family. For the map types, we apply the specification to the key or the value provided it
   * is one of the above primitive types. The specifier is a colon separated value of the form
   * -:s, or b:b where we have 's', 'b', or '-' on either side of the colon. 's' is for string
   * format storage, 'b' is for native fixed width byte oriented storage, and '-' uses the
   * table level default.
   *
   * @param hbaseTableDefaultStorageType - the specification associated with the table property
   *        hbase.table.default.storage.type
   * @throws SerDeException on parse error.
   */
  void parseColumnStorageTypes(String hbaseTableDefaultStorageType) throws SerDeException {

    boolean tableBinaryStorage = false;

    if (hbaseTableDefaultStorageType != null && !"".equals(hbaseTableDefaultStorageType)) {
      if (hbaseTableDefaultStorageType.equals("binary")) {
        tableBinaryStorage = true;
      } else if (!hbaseTableDefaultStorageType.equals("string")) {
        throw new SerDeException("Error: " + HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE +
            " parameter must be specified as" +
            " 'string' or 'binary'; '" + hbaseTableDefaultStorageType +
            "' is not a valid specification for this table/serde property.");
      }
    }

    // parse the string to determine column level storage type for primitive types
    // 's' is for variable length string format storage
    // 'b' is for fixed width binary storage of bytes
    // '-' is for table storage type, which defaults to UTF8 string
    // string data is always stored in the default escaped storage format; the data types
    // byte, short, int, long, float, and double have a binary byte oriented storage option
    for (ColumnMapping colMap : columnsMapping) {
      TypeInfo colType = colMap.columnType;
      String mappingSpec = colMap.mappingSpec;
      String[] mapInfo = mappingSpec.split("#");
      String[] storageInfo = null;

      if (mapInfo.length == 2) {
        storageInfo = mapInfo[1].split(":");
      }

      if (storageInfo == null) {

        // use the table default storage specification
        if (colType.getCategory() == ObjectInspector.Category.PRIMITIVE) {
          if (!colType.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else {
            colMap.binaryStorage.add(false);
          }
        } else if (colType.getCategory() == ObjectInspector.Category.MAP) {
          TypeInfo keyTypeInfo = ((MapTypeInfo) colType).getMapKeyTypeInfo();
          TypeInfo valueTypeInfo = ((MapTypeInfo) colType).getMapValueTypeInfo();

          if (keyTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE &&
              !keyTypeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else {
            colMap.binaryStorage.add(false);
          }

          if (valueTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE &&
              !valueTypeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else {
            colMap.binaryStorage.add(false);
          }
        } else {
          colMap.binaryStorage.add(false);
        }

      } else if (storageInfo.length == 1) {
        // we have a storage specification for a primitive column type
        String storageOption = storageInfo[0];

        if ((colType.getCategory() == ObjectInspector.Category.MAP) ||
            !(storageOption.equals("-") || "string".startsWith(storageOption) ||
                "binary".startsWith(storageOption))) {
          throw new SerDeException("Error: A column storage specification is one of the following:"
              + " '-', a prefix of 'string', or a prefix of 'binary'. "
              + storageOption + " is not a valid storage option specification for "
              + colMap.columnName);
        }

        if (colType.getCategory() == ObjectInspector.Category.PRIMITIVE &&
            !colType.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {

          if ("-".equals(storageOption)) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else if ("binary".startsWith(storageOption)) {
            colMap.binaryStorage.add(true);
          } else {
            colMap.binaryStorage.add(false);
          }
        } else {
          colMap.binaryStorage.add(false);
        }

      } else if (storageInfo.length == 2) {
        // we have a storage specification for a map column type

        String keyStorage = storageInfo[0];
        String valStorage = storageInfo[1];

        if ((colType.getCategory() != ObjectInspector.Category.MAP) ||
            !(keyStorage.equals("-") || "string".startsWith(keyStorage) ||
                "binary".startsWith(keyStorage)) ||
            !(valStorage.equals("-") || "string".startsWith(valStorage) ||
                "binary".startsWith(valStorage))) {
          throw new SerDeException("Error: To specify a valid column storage type for a Map"
              + " column, use any two specifiers from '-', a prefix of 'string', "
              + " and a prefix of 'binary' separated by a ':'."
              + " Valid examples are '-:-', 's:b', etc. They specify the storage type for the"
              + " key and value parts of the Map<?,?> respectively."
              + " Invalid storage specification for column "
              + colMap.columnName
              + "; " + storageInfo[0] + ":" + storageInfo[1]);
        }

        TypeInfo keyTypeInfo = ((MapTypeInfo) colType).getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = ((MapTypeInfo) colType).getMapValueTypeInfo();

        if (keyTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE &&
            !keyTypeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {

          if (keyStorage.equals("-")) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else if ("binary".startsWith(keyStorage)) {
            colMap.binaryStorage.add(true);
          } else {
            colMap.binaryStorage.add(false);
          }
        } else {
          colMap.binaryStorage.add(false);
        }

        if (valueTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE &&
            !valueTypeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
          if (valStorage.equals("-")) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else if ("binary".startsWith(valStorage)) {
            colMap.binaryStorage.add(true);
          } else {
            colMap.binaryStorage.add(false);
          }
        } else {
          colMap.binaryStorage.add(false);
        }

        if (colMap.binaryStorage.size() != 2) {
          throw new SerDeException("Error: In parsing the storage specification for column "
              + colMap.columnName);
        }

      } else {
        // error in storage specification
        throw new SerDeException("Error: " + HBaseSerDe.HBASE_COLUMNS_MAPPING + " storage specification "
            + mappingSpec + " is not valid for column: "
            + colMap.columnName);
      }
    }
  }

  public ColumnMapping getKeyMapping() {
    return columnsMapping[keyIndex];
  }

  public ColumnMapping getTimestampMapping() {
    return timestampIndex < 0 ? null : columnsMapping[timestampIndex];
  }

  public int getKeyIndex() {
    return keyIndex;
  }

  public int getTimestampIndex() {
    return timestampIndex;
  }

  public ColumnMapping[] getColumnsMapping() {
    return columnsMapping;
  }

  /**
   * Represents a mapping from a single Hive column to an HBase column qualifier, column family or row key.
   */
  // todo use final fields
  public static class ColumnMapping {

    ColumnMapping() {
      binaryStorage = new ArrayList<Boolean>(2);
    }

    String columnName;
    TypeInfo columnType;

    String familyName;
    String qualifierName;
    byte[] familyNameBytes;
    byte[] qualifierNameBytes;
    List<Boolean> binaryStorage;
    boolean hbaseRowKey;
    boolean hbaseTimestamp;
    String mappingSpec;
    String qualifierPrefix;
    byte[] qualifierPrefixBytes;

    public String getColumnName() {
      return columnName;
    }

    public TypeInfo getColumnType() {
      return columnType;
    }

    public String getFamilyName() {
      return familyName;
    }

    public String getQualifierName() {
      return qualifierName;
    }

    public byte[] getFamilyNameBytes() {
      return familyNameBytes;
    }

    public byte[] getQualifierNameBytes() {
      return qualifierNameBytes;
    }

    public List<Boolean> getBinaryStorage() {
      return binaryStorage;
    }

    public boolean isHbaseRowKey() {
      return hbaseRowKey;
    }

    public String getMappingSpec() {
      return mappingSpec;
    }

    public String getQualifierPrefix() {
      return qualifierPrefix;
    }

    public byte[] getQualifierPrefixBytes() {
      return qualifierPrefixBytes;
    }

    public boolean isCategory(ObjectInspector.Category category) {
      return columnType.getCategory() == category;
    }

    public boolean isCategory(PrimitiveCategory category) {
      return columnType.getCategory() == ObjectInspector.Category.PRIMITIVE &&
          ((PrimitiveTypeInfo)columnType).getPrimitiveCategory() == category;
    }

    public boolean isComparable() {
      return binaryStorage.get(0) || isCategory(PrimitiveCategory.STRING);
    }
  }
}
