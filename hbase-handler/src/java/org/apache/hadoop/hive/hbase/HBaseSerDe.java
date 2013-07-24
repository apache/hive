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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

/**
 * HBaseSerDe can be used to serialize object into an HBase table and
 * deserialize objects from an HBase table.
 */
public class HBaseSerDe extends AbstractSerDe {

  public static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";
  public static final String HBASE_TABLE_NAME = "hbase.table.name";
  public static final String HBASE_TABLE_DEFAULT_STORAGE_TYPE = "hbase.table.default.storage.type";
  public static final String HBASE_KEY_COL = ":key";
  public static final String HBASE_PUT_TIMESTAMP = "hbase.put.timestamp";
  public static final String HBASE_SCAN_CACHE = "hbase.scan.cache";
  public static final String HBASE_SCAN_CACHEBLOCKS = "hbase.scan.cacheblock";
  public static final String HBASE_SCAN_BATCH = "hbase.scan.batch";
  
  /** Determines whether a regex matching should be done on the columns or not. Defaults to true. 
   *  <strong>WARNING: Note that currently this only supports the suffix wildcard .*</strong> **/
  public static final String HBASE_COLUMNS_REGEX_MATCHING = "hbase.columns.mapping.regex.matching";

  public static final Log LOG = LogFactory.getLog(HBaseSerDe.class);

  private ObjectInspector cachedObjectInspector;
  private String hbaseColumnsMapping;
  private boolean doColumnRegexMatching;
  private List<ColumnMapping> columnsMapping;
  private SerDeParameters serdeParams;
  private boolean useJSONSerialize;
  private LazyHBaseRow cachedHBaseRow;
  private final ByteStream.Output serializeStream = new ByteStream.Output();
  private int iKey;
  private long putTimestamp;

  // used for serializing a field
  private byte [] separators;     // the separators array
  private boolean escaped;        // whether we need to escape the data when writing out
  private byte escapeChar;        // which char to use as the escape char, e.g. '\\'
  private boolean [] needsEscape; // which chars need to be escaped. This array should have size
                                  // of 128. Negative byte values (or byte values >= 128) are
                                  // never escaped.
  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + hbaseColumnsMapping
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldTypeInfos() + "]";
  }

  public HBaseSerDe() throws SerDeException {
  }

  /**
   * Initialize the SerDe given parameters.
   * @see SerDe#initialize(Configuration, Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    initHBaseSerDeParameters(conf, tbl, getClass().getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes(),
        serdeParams.getSeparators(),
        serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(),
        serdeParams.getEscapeChar());

    cachedHBaseRow = new LazyHBaseRow(
      (LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("HBaseSerDe initialized with : columnNames = "
        + serdeParams.getColumnNames()
        + " columnTypes = "
        + serdeParams.getColumnTypes()
        + " hbaseColumnMapping = "
        + hbaseColumnsMapping);
    }
  }

  /**
   * Parses the HBase columns mapping specifier to identify the column families, qualifiers
   * and also caches the byte arrays corresponding to them. One of the Hive table
   * columns maps to the HBase row key, by default the first column.
   *
   * @param columnsMappingSpec string hbase.columns.mapping specified when creating table
   * @return List<ColumnMapping> which contains the column mapping information by position
   * @throws SerDeException
   */
  public static List<ColumnMapping> parseColumnsMapping(String columnsMappingSpec)
      throws SerDeException {
    return parseColumnsMapping(columnsMappingSpec, true);
  }

  /**
   * Parses the HBase columns mapping specifier to identify the column families, qualifiers
   * and also caches the byte arrays corresponding to them. One of the Hive table
   * columns maps to the HBase row key, by default the first column.
   *
   * @param columnsMappingSpec string hbase.columns.mapping specified when creating table
   * @param doColumnRegexMatching whether to do a regex matching on the columns or not
   * @return List<ColumnMapping> which contains the column mapping information by position
   * @throws SerDeException
   */
  public static List<ColumnMapping> parseColumnsMapping(String columnsMappingSpec, boolean doColumnRegexMatching)
      throws SerDeException {

    if (columnsMappingSpec == null) {
      throw new SerDeException("Error: hbase.columns.mapping missing for this HBase table.");
    }

    if (columnsMappingSpec.equals("") || columnsMappingSpec.equals(HBASE_KEY_COL)) {
      throw new SerDeException("Error: hbase.columns.mapping specifies only the HBase table"
          + " row key. A valid Hive-HBase table must specify at least one additional column.");
    }

    int rowKeyIndex = -1;
    List<ColumnMapping> columnsMapping = new ArrayList<ColumnMapping>();
    String [] columnSpecs = columnsMappingSpec.split(",");
    ColumnMapping columnMapping = null;

    for (int i = 0; i < columnSpecs.length; i++) {
      String mappingSpec = columnSpecs[i].trim();
      String [] mapInfo = mappingSpec.split("#");
      String colInfo = mapInfo[0];

      int idxFirst = colInfo.indexOf(":");
      int idxLast = colInfo.lastIndexOf(":");

      if (idxFirst < 0 || !(idxFirst == idxLast)) {
        throw new SerDeException("Error: the HBase columns mapping contains a badly formed " +
            "column family, column qualifier specification.");
      }

      columnMapping = new ColumnMapping();

      if (colInfo.equals(HBASE_KEY_COL)) {
        rowKeyIndex = i;
        columnMapping.familyName = colInfo;
        columnMapping.familyNameBytes = Bytes.toBytes(colInfo);
        columnMapping.qualifierName = null;
        columnMapping.qualifierNameBytes = null;
        columnMapping.hbaseRowKey = true;
      } else {
        String [] parts = colInfo.split(":");
        assert(parts.length > 0 && parts.length <= 2);
        columnMapping.familyName = parts[0];
        columnMapping.familyNameBytes = Bytes.toBytes(parts[0]);
        columnMapping.hbaseRowKey = false;

        if (parts.length == 2) {

          if (doColumnRegexMatching && parts[1].endsWith(".*")) {
            // we have a prefix with a wildcard
            columnMapping.qualifierPrefix = parts[1].substring(0, parts[1].length() - 2);
            columnMapping.qualifierPrefixBytes = Bytes.toBytes(columnMapping.qualifierPrefix);
            // we weren't provided any actual qualifier name. Set these to
            // null.
            columnMapping.qualifierName = null;
            columnMapping.qualifierNameBytes = null;
          } else {
            // set the regular provided qualifier names
            columnMapping.qualifierName = parts[1];
            columnMapping.qualifierNameBytes = Bytes.toBytes(parts[1]);
            ;
          }
        } else {
          columnMapping.qualifierName = null;
          columnMapping.qualifierNameBytes = null;
        }
      }

      columnMapping.mappingSpec = mappingSpec;

      columnsMapping.add(columnMapping);
    }

    if (rowKeyIndex == -1) {
      columnMapping = new ColumnMapping();
      columnMapping.familyName = HBASE_KEY_COL;
      columnMapping.familyNameBytes = Bytes.toBytes(HBASE_KEY_COL);
      columnMapping.qualifierName = null;
      columnMapping.qualifierNameBytes = null;
      columnMapping.hbaseRowKey = true;
      columnMapping.mappingSpec = HBASE_KEY_COL;
      columnsMapping.add(0, columnMapping);
    }

    return columnsMapping;
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

  private void parseColumnStorageTypes(String hbaseTableDefaultStorageType)
      throws SerDeException {

    boolean tableBinaryStorage = false;

    if (hbaseTableDefaultStorageType != null && !"".equals(hbaseTableDefaultStorageType)) {
      if (hbaseTableDefaultStorageType.equals("binary")) {
        tableBinaryStorage = true;
      } else if (!hbaseTableDefaultStorageType.equals("string")) {
        throw new SerDeException("Error: " + HBASE_TABLE_DEFAULT_STORAGE_TYPE +
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

    for (int i = 0; i < columnsMapping.size(); i++) {

      ColumnMapping colMap = columnsMapping.get(i);
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
        throw new SerDeException("Error: " + HBASE_COLUMNS_MAPPING + " storage specification "
            + mappingSpec + " is not valid for column: "
            + serdeParams.getColumnNames().get(i));
      }
    }
  }


  public static boolean isRowKeyColumn(String hbaseColumnName) {
    return hbaseColumnName.equals(HBASE_KEY_COL);
  }


  static class ColumnMapping {

    ColumnMapping() {
      binaryStorage = new ArrayList<Boolean>(2);
    }

    String familyName;
    String qualifierName;
    byte [] familyNameBytes;
    byte [] qualifierNameBytes;
    List<Boolean> binaryStorage;
    boolean hbaseRowKey;
    String mappingSpec;
    String qualifierPrefix;
    byte[] qualifierPrefixBytes;
  }

  private void initHBaseSerDeParameters(
      Configuration job, Properties tbl, String serdeName)
    throws SerDeException {

    // Read configuration parameters
    hbaseColumnsMapping = tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    putTimestamp = Long.valueOf(tbl.getProperty(HBaseSerDe.HBASE_PUT_TIMESTAMP,"-1"));

    doColumnRegexMatching = Boolean.valueOf(tbl.getProperty(HBASE_COLUMNS_REGEX_MATCHING, "true"));

    // Parse and initialize the HBase columns mapping
    columnsMapping = parseColumnsMapping(hbaseColumnsMapping, doColumnRegexMatching);

    // Build the type property string if not supplied
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < columnsMapping.size(); i++) {
        if (sb.length() > 0) {
          sb.append(":");
        }

        ColumnMapping colMap = columnsMapping.get(i);

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

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

    if (columnsMapping.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
        serdeParams.getColumnNames().size() +
        " elements while hbase.columns.mapping has " +
        columnsMapping.size() + " elements" +
        " (counting the key if implicit)");
    }

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();

    // check that the mapping schema is right;
    // check that the "column-family:" is mapped to  Map<key,?>
    // where key extends LazyPrimitive<?, ?> and thus has type Category.PRIMITIVE
    for (int i = 0; i < columnsMapping.size(); i++) {
      ColumnMapping colMap = columnsMapping.get(i);
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

  /**
   * Deserialize a row from the HBase Result writable to a LazyObject
   * @param result the HBase Result Writable containing the row
   * @return the deserialized object
   * @see SerDe#deserialize(Writable)
   */
  @Override
  public Object deserialize(Writable result) throws SerDeException {

    if (!(result instanceof Result)) {
      throw new SerDeException(getClass().getName() + ": expects Result!");
    }

    cachedHBaseRow.init((Result) result, columnsMapping);

    return cachedHBaseRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Put.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields =
      (serdeParams.getRowTypeInfo() != null &&
        ((StructTypeInfo) serdeParams.getRowTypeInfo())
        .getAllStructFieldNames().size() > 0) ?
      ((StructObjectInspector)getObjectInspector()).getAllStructFieldRefs()
      : null;

    Put put = null;

    try {
      byte [] key = serializeField(iKey, null, fields, list, declaredFields);

      if (key == null) {
        throw new SerDeException("HBase row key cannot be NULL");
      }

      if(putTimestamp >= 0) {
        put = new Put(key,putTimestamp);
      } else {
        put = new Put(key);
      }

      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        if (i == iKey) {
          // already processed the key above
          continue;
        }
        serializeField(i, put, fields, list, declaredFields);
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return put;
  }

  private byte [] serializeField(
    int i,
    Put put,
    List<? extends StructField> fields,
    List<Object> list,
    List<? extends StructField> declaredFields) throws IOException {

    // column mapping info
    ColumnMapping colMap = columnsMapping.get(i);

    // Get the field objectInspector and the field object.
    ObjectInspector foi = fields.get(i).getFieldObjectInspector();
    Object f = (list == null ? null : list.get(i));

    if (f == null) {
      // a null object, we do not serialize it
      return null;
    }

    // If the field corresponds to a column family in HBase
    if (colMap.qualifierName == null && !colMap.hbaseRowKey) {
      MapObjectInspector moi = (MapObjectInspector) foi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(f);
      if (map == null) {
        return null;
      } else {
        for (Map.Entry<?, ?> entry: map.entrySet()) {
          // Get the Key
          serializeStream.reset();

          // Map keys are required to be primitive and may be serialized in binary format
          boolean isNotNull = serialize(entry.getKey(), koi, 3, colMap.binaryStorage.get(0));
          if (!isNotNull) {
            continue;
          }

          // Get the column-qualifier
          byte [] columnQualifierBytes = new byte[serializeStream.getCount()];
          System.arraycopy(
              serializeStream.getData(), 0, columnQualifierBytes, 0, serializeStream.getCount());

          // Get the Value
          serializeStream.reset();

          // Map values may be serialized in binary format when they are primitive and binary
          // serialization is the option selected
          isNotNull = serialize(entry.getValue(), voi, 3, colMap.binaryStorage.get(1));
          if (!isNotNull) {
            continue;
          }
          byte [] value = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());
          put.add(colMap.familyNameBytes, columnQualifierBytes, value);
        }
      }
    } else {
      // If the field that is passed in is NOT a primitive, and either the
      // field is not declared (no schema was given at initialization), or
      // the field is declared as a primitive in initialization, serialize
      // the data to JSON string.  Otherwise serialize the data in the
      // delimited way.
      serializeStream.reset();
      boolean isNotNull;
      if (!foi.getCategory().equals(Category.PRIMITIVE)
          && (declaredFields == null ||
              declaredFields.get(i).getFieldObjectInspector().getCategory()
              .equals(Category.PRIMITIVE) || useJSONSerialize)) {

        // we always serialize the String type using the escaped algorithm for LazyString
        isNotNull = serialize(
            SerDeUtils.getJSONString(f, foi),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            1, false);
      } else {
        // use the serialization option switch to write primitive values as either a variable
        // length UTF8 string or a fixed width bytes if serializing in binary format
        isNotNull = serialize(f, foi, 1, colMap.binaryStorage.get(0));
      }
      if (!isNotNull) {
        return null;
      }
      byte [] key = new byte[serializeStream.getCount()];
      System.arraycopy(serializeStream.getData(), 0, key, 0, serializeStream.getCount());
      if (i == iKey) {
        return key;
      }
      put.add(colMap.familyNameBytes, colMap.qualifierNameBytes, key);
    }

    return null;
  }

  /*
   * Serialize the row into a ByteStream.
   *
   * @param obj           The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param level         The current level of separator.
   * @param writeBinary   Whether to write a primitive object as an UTF8 variable length string or
   *                      as a fixed width byte array onto the byte stream.
   * @throws IOException  On error in writing to the serialization stream.
   * @return true         On serializing a non-null object, otherwise false.
   */
  private boolean serialize(
      Object obj,
      ObjectInspector objInspector,
      int level,
      boolean writeBinary) throws IOException {

    if (objInspector.getCategory() == Category.PRIMITIVE && writeBinary) {
      LazyUtils.writePrimitive(serializeStream, obj, (PrimitiveObjectInspector) objInspector);
      return true;
    } else {
      return serialize(obj, objInspector, level);
    }
  }

  private boolean serialize(
      Object obj,
      ObjectInspector objInspector,
      int level) throws IOException {

    switch (objInspector.getCategory()) {
      case PRIMITIVE: {
        LazyUtils.writePrimitiveUTF8(serializeStream, obj,
            (PrimitiveObjectInspector) objInspector, escaped, escapeChar, needsEscape);
        return true;
      }

      case LIST: {
        char separator = (char) separators[level];
        ListObjectInspector loi = (ListObjectInspector)objInspector;
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              serializeStream.write(separator);
            }
            serialize(list.get(i), eoi, level + 1);
          }
        }
        return true;
      }

      case MAP: {
        char separator = (char) separators[level];
        char keyValueSeparator = (char) separators[level+1];
        MapObjectInspector moi = (MapObjectInspector) objInspector;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();

        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
          return false;
        } else {
          boolean first = true;
          for (Map.Entry<?, ?> entry: map.entrySet()) {
            if (first) {
              first = false;
            } else {
              serializeStream.write(separator);
            }
            serialize(entry.getKey(), koi, level+2);
            serializeStream.write(keyValueSeparator);
            serialize(entry.getValue(), voi, level+2);
          }
        }
        return true;
      }

      case STRUCT: {
        char separator = (char)separators[level];
        StructObjectInspector soi = (StructObjectInspector)objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              serializeStream.write(separator);
            }

            serialize(list.get(i), fields.get(i).getFieldObjectInspector(),
                level + 1);
          }
        }
        return true;
      }
    }

    throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
  }


  /**
   * @return the useJSONSerialize
   */
  public boolean isUseJSONSerialize() {
    return useJSONSerialize;
  }

  /**
   * @param useJSONSerialize the useJSONSerialize to set
   */
  public void setUseJSONSerialize(boolean useJSONSerialize) {
    this.useJSONSerialize = useJSONSerialize;
  }

  /**
   * @return 0-based offset of the key column within the table
   */
  int getKeyColumnOffset() {
    return iKey;
  }

  List<Boolean> getStorageFormatOfCol(int colPos){
    return columnsMapping.get(colPos).binaryStorage;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  void setKeyColumnOffset() throws SerDeException {
    iKey = getRowKeyColumnOffset(columnsMapping);
  }

  public static int getRowKeyColumnOffset(List<ColumnMapping> columnsMapping)
      throws SerDeException {

    for (int i = 0; i < columnsMapping.size(); i++) {
      ColumnMapping colMap = columnsMapping.get(i);

      if (colMap.hbaseRowKey && colMap.familyName.equals(HBASE_KEY_COL)) {
        return i;
      }
    }

    throw new SerDeException("HBaseSerDe Error: columns mapping list does not contain" +
      " row key column.");
  }
}
