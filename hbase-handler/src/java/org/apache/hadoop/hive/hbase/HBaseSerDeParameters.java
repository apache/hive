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

package org.apache.hadoop.hive.hbase;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.HBaseSerDe.ColumnMapping;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * HBaseSerDeParameters encapsulates SerDeParameters and additional configurations that are specific for
 * HBaseSerDe.
 *
 */
public class HBaseSerDeParameters {
  private SerDeParameters serdeParams;

  private String columnMappingString;
  private List<ColumnMapping> columnMapping;
  private boolean doColumnRegexMatching;

  private long putTimestamp;

  private Class<?> compositeKeyClass;
  private int keyIndex;

  void init(Configuration job, Properties tbl, String serdeName) throws SerDeException {
    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);
    putTimestamp = Long.valueOf(tbl.getProperty(HBaseSerDe.HBASE_PUT_TIMESTAMP, "-1"));

    String compKeyClass = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS);
    if (compKeyClass != null) {
      try {
        compositeKeyClass = job.getClassByName(compKeyClass);
      } catch (ClassNotFoundException e) {
        throw new SerDeException(e);
      }
    }

    // Read configuration parameters
    columnMappingString = tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    doColumnRegexMatching = Boolean.valueOf(tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, "true"));
    // Parse and initialize the HBase columns mapping
    columnMapping = HBaseSerDe.parseColumnsMapping(columnMappingString, doColumnRegexMatching);

    // Build the type property string if not supplied
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < columnMapping.size(); i++) {
        if (sb.length() > 0) {
          sb.append(":");
        }

        ColumnMapping colMap = columnMapping.get(i);

        if (colMap.hbaseRowKey) {
          // the row key column becomes a STRING
          sb.append(serdeConstants.STRING_TYPE_NAME);
        } else if (colMap.qualifierName == null)  {
          // a column family become a MAP
          sb.append(serdeConstants.MAP_TYPE_NAME + "<" + serdeConstants.STRING_TYPE_NAME + ","
              + serdeConstants.STRING_TYPE_NAME + ">");
        } else {
          // an individual column becomes a STRING
          sb.append(serdeConstants.STRING_TYPE_NAME);
        }
      }
      tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, sb.toString());
    }

    if (columnMapping.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
        serdeParams.getColumnNames().size() +
        " elements while hbase.columns.mapping has " +
        columnMapping.size() + " elements" +
        " (counting the key if implicit)");
    }

    // check that the mapping schema is right;
    // check that the "column-family:" is mapped to  Map<key,?>
    // where key extends LazyPrimitive<?, ?> and thus has type Category.PRIMITIVE
    for (int i = 0; i < columnMapping.size(); i++) {
      ColumnMapping colMap = columnMapping.get(i);
      if (colMap.qualifierName == null && !colMap.hbaseRowKey) {
        TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
        if ((typeInfo.getCategory() != Category.MAP) ||
          (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getCategory()
            !=  Category.PRIMITIVE)) {

          throw new SerDeException(
            serdeName + ": hbase column family '" + colMap.familyName
            + "' should be mapped to Map<? extends LazyPrimitive<?, ?>,?>, that is "
            + "the Key for the map should be of primitive type, but is mapped to "
            + typeInfo.getTypeName());
        }
      }
    }

    // Precondition: make sure this is done after the rest of the SerDe initialization is done.
    String hbaseTableStorageType = tbl.getProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE);
    parseColumnStorageTypes(hbaseTableStorageType);
    setKeyColumnOffset();
  }

  /*
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

  public void parseColumnStorageTypes(String hbaseTableDefaultStorageType)
      throws SerDeException {

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
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();

    for (int i = 0; i < columnMapping.size(); i++) {

      ColumnMapping colMap = columnMapping.get(i);
      TypeInfo colType = columnTypes.get(i);
      String mappingSpec = colMap.mappingSpec;
      String [] mapInfo = mappingSpec.split("#");
      String [] storageInfo = null;

      if (mapInfo.length == 2) {
        storageInfo = mapInfo[1].split(":");
      }

      if (storageInfo == null) {

        // use the table default storage specification
        if (colType.getCategory() == Category.PRIMITIVE) {
          if (!colType.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else {
            colMap.binaryStorage.add(false);
          }
        } else if (colType.getCategory() == Category.MAP) {
          TypeInfo keyTypeInfo = ((MapTypeInfo) colType).getMapKeyTypeInfo();
          TypeInfo valueTypeInfo = ((MapTypeInfo) colType).getMapValueTypeInfo();

          if (keyTypeInfo.getCategory() == Category.PRIMITIVE &&
              !keyTypeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            colMap.binaryStorage.add(tableBinaryStorage);
          } else {
            colMap.binaryStorage.add(false);
          }

          if (valueTypeInfo.getCategory() == Category.PRIMITIVE &&
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

        if ((colType.getCategory() == Category.MAP) ||
            !(storageOption.equals("-") || "string".startsWith(storageOption) ||
                "binary".startsWith(storageOption))) {
          throw new SerDeException("Error: A column storage specification is one of the following:"
              + " '-', a prefix of 'string', or a prefix of 'binary'. "
              + storageOption + " is not a valid storage option specification for "
              + serdeParams.getColumnNames().get(i));
        }

        if (colType.getCategory() == Category.PRIMITIVE &&
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

        if ((colType.getCategory() != Category.MAP) ||
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
              + serdeParams.getColumnNames().get(i)
              + "; " + storageInfo[0] + ":" + storageInfo[1]);
        }

        TypeInfo keyTypeInfo = ((MapTypeInfo) colType).getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = ((MapTypeInfo) colType).getMapValueTypeInfo();

        if (keyTypeInfo.getCategory() == Category.PRIMITIVE &&
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

        if (valueTypeInfo.getCategory() == Category.PRIMITIVE &&
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
              + serdeParams.getColumnNames().get(i));
        }

      } else {
        // error in storage specification
        throw new SerDeException("Error: " + HBaseSerDe.HBASE_COLUMNS_MAPPING + " storage specification "
            + mappingSpec + " is not valid for column: "
            + serdeParams.getColumnNames().get(i));
      }
    }
  }

  void setKeyColumnOffset() throws SerDeException {
    setKeyIndex(getRowKeyColumnOffset(columnMapping));
  }

  public static int getRowKeyColumnOffset(List<ColumnMapping> columnsMapping)
      throws SerDeException {

    for (int i = 0; i < columnsMapping.size(); i++) {
      ColumnMapping colMap = columnsMapping.get(i);

      if (colMap.hbaseRowKey && colMap.familyName.equals(HBaseSerDe.HBASE_KEY_COL)) {
        return i;
      }
    }

    throw new SerDeException("HBaseSerDe Error: columns mapping list does not contain" +
      " row key column.");
  }

  public StructTypeInfo getRowTypeInfo() {
    return (StructTypeInfo) serdeParams.getRowTypeInfo();
  }

  public List<String> getColumnNames() {
    return serdeParams.getColumnNames();
  }

  public byte[] getSeparators() {
    return serdeParams.getSeparators();
  }

  public Text getNullSequence() {
    return serdeParams.getNullSequence();
  }

  public boolean isLastColumnTakesRest() {
    return serdeParams.isLastColumnTakesRest();
  }

  public boolean isEscaped() {
    return serdeParams.isEscaped();
  }

  public byte getEscapeChar() {
    return serdeParams.getEscapeChar();
  }

  public List<TypeInfo> getColumnTypes() {
    return serdeParams.getColumnTypes();
  }

  public SerDeParameters getSerdeParams() {
    return serdeParams;
  }

  public void setSerdeParams(SerDeParameters serdeParams) {
    this.serdeParams = serdeParams;
  }

  public String getColumnMappingString() {
    return columnMappingString;
  }

  public void setColumnMappingString(String columnMappingString) {
    this.columnMappingString = columnMappingString;
  }

  public long getPutTimestamp() {
    return putTimestamp;
  }

  public void setPutTimestamp(long putTimestamp) {
    this.putTimestamp = putTimestamp;
  }

  public boolean isDoColumnRegexMatching() {
    return doColumnRegexMatching;
  }

  public void setDoColumnRegexMatching(boolean doColumnRegexMatching) {
    this.doColumnRegexMatching = doColumnRegexMatching;
  }

  public Class<?> getCompositeKeyClass() {
    return compositeKeyClass;
  }

  public void setCompositeKeyClass(Class<?> compositeKeyClass) {
    this.compositeKeyClass = compositeKeyClass;
  }

  public int getKeyIndex() {
    return keyIndex;
  }

  public void setKeyIndex(int keyIndex) {
    this.keyIndex = keyIndex;
  }

  public List<ColumnMapping> getColumnMapping() {
    return columnMapping;
  }

  public ColumnMapping getKeyColumnMapping() {
      return columnMapping.get(keyIndex);
    }

  public boolean[] getNeedsEscape() {
    return serdeParams.getNeedsEscape();
  }
}
