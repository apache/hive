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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Writable;

public class HBaseRowSerializer {

  private final HBaseKeyFactory keyFactory;
  private final HBaseSerDeParameters hbaseParam;
  private final LazySerDeParameters serdeParam;

  private final int keyIndex;
  private final int timestampIndex;
  private final ColumnMapping keyMapping;
  private final ColumnMapping timestampMapping;
  private final ColumnMapping[] columnMappings;
  private final byte[] separators;      // the separators array
  private final boolean escaped;        // whether we need to escape the data when writing out
  private final byte escapeChar;        // which char to use as the escape char, e.g. '\\'
  private final boolean[] needsEscape;  // which chars need to be escaped. 

  private final long putTimestamp;
  private final ByteStream.Output output = new ByteStream.Output();

  public HBaseRowSerializer(HBaseSerDeParameters hbaseParam) {
    this.hbaseParam = hbaseParam;
    this.keyFactory = hbaseParam.getKeyFactory();
    this.serdeParam = hbaseParam.getSerdeParams();
    this.separators = serdeParam.getSeparators();
    this.escaped = serdeParam.isEscaped();
    this.escapeChar = serdeParam.getEscapeChar();
    this.needsEscape = serdeParam.getNeedsEscape();
    this.keyIndex = hbaseParam.getKeyIndex();
    this.timestampIndex = hbaseParam.getTimestampIndex();
    this.columnMappings = hbaseParam.getColumnMappings().getColumnsMapping();
    this.keyMapping = hbaseParam.getColumnMappings().getKeyMapping();
    this.timestampMapping = hbaseParam.getColumnMappings().getTimestampMapping();
    this.putTimestamp = hbaseParam.getPutTimestamp();
  }

  public Writable serialize(Object obj, ObjectInspector objInspector) throws Exception {
    if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> values = soi.getStructFieldsDataAsList(obj);

    StructField field = fields.get(keyIndex);
    Object value = values.get(keyIndex);

    byte[] key = keyFactory.serializeKey(value, field);
    if (key == null) {
      throw new SerDeException("HBase row key cannot be NULL");
    }
    long timestamp = putTimestamp;
    if (timestamp < 0 && timestampIndex >= 0) {
      ObjectInspector inspector = fields.get(timestampIndex).getFieldObjectInspector();
      value = values.get(timestampIndex);
      if (inspector instanceof LongObjectInspector) {
        timestamp = ((LongObjectInspector)inspector).get(value);
      } else {
        PrimitiveObjectInspector primitive = (PrimitiveObjectInspector) inspector;
        timestamp = PrimitiveObjectInspectorUtils.getTimestamp(value, primitive).getTime();
      }
    }

    Put put = timestamp >= 0 ? new Put(key, timestamp) : new Put(key);

    // Serialize each field
    for (int i = 0; i < fields.size(); i++) {
      if (i == keyIndex || i == timestampIndex) {
        continue;
      }
      field = fields.get(i);
      value = values.get(i);
      serializeField(value, field, columnMappings[i], put);
    }

    return new PutWritable(put);
  }

  byte[] serializeKeyField(Object keyValue, StructField keyField, ColumnMapping keyMapping)
      throws IOException {
    if (keyValue == null) {
      throw new IOException("HBase row key cannot be NULL");
    }
    ObjectInspector keyFieldOI = keyField.getFieldObjectInspector();

    if (!keyFieldOI.getCategory().equals(ObjectInspector.Category.PRIMITIVE) &&
        keyMapping.isCategory(ObjectInspector.Category.PRIMITIVE)) {
      // we always serialize the String type using the escaped algorithm for LazyString
      return serialize(SerDeUtils.getJSONString(keyValue, keyFieldOI),
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1, false);
    }
    // use the serialization option switch to write primitive values as either a variable
    // length UTF8 string or a fixed width bytes if serializing in binary format
    boolean writeBinary = keyMapping.binaryStorage.get(0);
    return serialize(keyValue, keyFieldOI, 1, writeBinary);
  }

  private void serializeField(
      Object value, StructField field, ColumnMapping colMap, Put put) throws IOException {
    if (value == null) {
      // a null object, we do not serialize it
      return;
    }
    // Get the field objectInspector and the field object.
    ObjectInspector foi = field.getFieldObjectInspector();

    // If the field corresponds to a column family in HBase
    if (colMap.qualifierName == null) {
      MapObjectInspector moi = (MapObjectInspector) foi;
      Map<?, ?> map = moi.getMap(value);
      if (map == null) {
        return;
      }
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      for (Map.Entry<?, ?> entry: map.entrySet()) {
        // Get the Key
        // Map keys are required to be primitive and may be serialized in binary format
        byte[] columnQualifierBytes = serialize(entry.getKey(), koi, 3, colMap.binaryStorage.get(0));
        if (columnQualifierBytes == null) {
          continue;
        }

        // Map values may be serialized in binary format when they are primitive and binary
        // serialization is the option selected
        byte[] bytes = serialize(entry.getValue(), voi, 3, colMap.binaryStorage.get(1));
        if (bytes == null) {
          continue;
        }

        put.addColumn(colMap.familyNameBytes, columnQualifierBytes, bytes);
      }
    } else {
      byte[] bytes;
      // If the field that is passed in is NOT a primitive, and either the
      // field is not declared (no schema was given at initialization), or
      // the field is declared as a primitive in initialization, serialize
      // the data to JSON string.  Otherwise serialize the data in the
      // delimited way.
      if (!foi.getCategory().equals(ObjectInspector.Category.PRIMITIVE)
          && colMap.isCategory(ObjectInspector.Category.PRIMITIVE)) {
        // we always serialize the String type using the escaped algorithm for LazyString
        bytes = serialize(SerDeUtils.getJSONString(value, foi),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1, false);
      } else {
        // use the serialization option switch to write primitive values as either a variable
        // length UTF8 string or a fixed width bytes if serializing in binary format
        bytes = serialize(value, foi, 1, colMap.binaryStorage.get(0));
      }

      if (bytes == null) {
        return;
      }

      put.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes, bytes);
    }
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
  private byte[] serialize(Object obj, ObjectInspector objInspector, int level, boolean writeBinary)
      throws IOException {
    output.reset();
    if (objInspector.getCategory() == ObjectInspector.Category.PRIMITIVE && writeBinary) {
      LazyUtils.writePrimitive(output, obj, (PrimitiveObjectInspector) objInspector);
    } else {
      if (!serialize(obj, objInspector, level, output)) {
        return null;
      }
    }
    return output.toByteArray();
  }

  private boolean serialize(
      Object obj,
      ObjectInspector objInspector,
      int level, ByteStream.Output ss) throws IOException {

    switch (objInspector.getCategory()) {
      case PRIMITIVE:
        LazyUtils.writePrimitiveUTF8(ss, obj,
            (PrimitiveObjectInspector) objInspector, escaped, escapeChar, needsEscape);
        return true;
      case LIST:
        char separator = (char) separators[level];
        ListObjectInspector loi = (ListObjectInspector)objInspector;
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              ss.write(separator);
            }
            serialize(list.get(i), eoi, level + 1, ss);
          }
        }
        return true;
      case MAP:
        char sep = (char) separators[level];
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
              ss.write(sep);
            }
            serialize(entry.getKey(), koi, level+2, ss);

            if ( entry.getValue() != null) {
              ss.write(keyValueSeparator);
              serialize(entry.getValue(), voi, level+2, ss);
            }
          }
        }
        return true;
      case STRUCT:
        sep = (char)separators[level];
        StructObjectInspector soi = (StructObjectInspector)objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              ss.write(sep);
            }

            serialize(list.get(i), fields.get(i).getFieldObjectInspector(),
                level + 1, ss);
          }
        }
        return true;
       case UNION: {
        // union type currently not totally supported. See HIVE-2390
        return false;
       }
      default:
        throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
    }
  }
}
