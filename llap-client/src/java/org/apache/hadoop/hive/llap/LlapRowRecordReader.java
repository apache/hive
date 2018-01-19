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

package org.apache.hadoop.hive.llap;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Row-based record reader for LLAP.
 */
public class LlapRowRecordReader implements RecordReader<NullWritable, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapRowRecordReader.class);

  protected final Configuration conf;
  protected final RecordReader<NullWritable, BytesWritable> reader;
  protected final Schema schema;
  protected final AbstractSerDe serde;
  protected final BytesWritable data;

  public LlapRowRecordReader(Configuration conf, Schema schema,
      RecordReader<NullWritable, BytesWritable> reader) throws IOException {
    this.conf = conf;
    this.schema = schema;
    this.reader = reader;
    this.data = new BytesWritable();

    try {
      serde = initSerDe(conf);
    } catch (SerDeException err) {
      throw new IOException(err);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public Row createValue() {
    return new Row(schema);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  @Override
  public boolean next(NullWritable key, Row value) throws IOException {
    Preconditions.checkArgument(value != null);

    boolean hasNext = reader.next(key,  data);
    if (hasNext) {
      // Deserialize data to column values, and populate the row record
      Object rowObj;
      try {
        StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
        rowObj = serde.deserialize(data);
        setRowFromStruct(value, rowObj, rowOI);
      } catch (SerDeException err) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Error deserializing row from data: " + data);
        }
        throw new IOException("Error deserializing row data", err);
      }
    }

    return hasNext;
  }

  public Schema getSchema() {
    return schema;
  }

  static Object convertPrimitive(Object val, PrimitiveObjectInspector poi) {
    switch (poi.getPrimitiveCategory()) {
    // Save char/varchar as string
    case CHAR:
      return ((HiveChar) poi.getPrimitiveJavaObject(val)).getPaddedValue();
    case VARCHAR:
      return ((HiveVarchar) poi.getPrimitiveJavaObject(val)).toString();
    case DECIMAL:
      return ((HiveDecimal) poi.getPrimitiveJavaObject(val)).bigDecimalValue();
    default:
      return poi.getPrimitiveJavaObject(val);
    }
  }

  static Object convertValue(Object val, ObjectInspector oi) {
    if (val == null) {
      return null;
    }

    Object convertedVal = null;
    ObjectInspector.Category oiCategory = oi.getCategory();
    switch (oiCategory) {
    case PRIMITIVE:
      convertedVal = convertPrimitive(val, (PrimitiveObjectInspector) oi);
      break;
    case LIST:
      ListObjectInspector loi = (ListObjectInspector) oi;
      int listSize = loi.getListLength(val);
      // Per ListObjectInpsector.getListLength(), -1 length means null list.
      if (listSize < 0) {
        return null;
      }
      List<Object> convertedList = new ArrayList<Object>(listSize);
      ObjectInspector listElementOI = loi.getListElementObjectInspector();
      for (int idx = 0; idx < listSize; ++idx) {
        convertedList.add(convertValue(loi.getListElement(val, idx), listElementOI));
      }
      convertedVal = convertedList;
      break;
    case MAP:
      MapObjectInspector moi = (MapObjectInspector) oi;
      int mapSize = moi.getMapSize(val);
      // Per MapObjectInpsector.getMapSize(), -1 length means null map.
      if (mapSize < 0) {
        return null;
      }
      Map<Object, Object> convertedMap = new LinkedHashMap<Object, Object>(mapSize);
      ObjectInspector mapKeyOI = moi.getMapKeyObjectInspector();
      ObjectInspector mapValOI = moi.getMapValueObjectInspector();
      Map<?, ?> mapCol = moi.getMap(val);
      for (Object mapKey : mapCol.keySet()) {
        Object convertedMapKey = convertValue(mapKey, mapKeyOI);
        Object convertedMapVal = convertValue(mapCol.get(mapKey), mapValOI);
        convertedMap.put(convertedMapKey,  convertedMapVal);
      }
      convertedVal = convertedMap;
      break;
    case STRUCT:
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<Object> convertedRow = new ArrayList<Object>();
      for (StructField structField : soi.getAllStructFieldRefs()) {
        Object convertedFieldValue = convertValue(
            soi.getStructFieldData(val, structField),
            structField.getFieldObjectInspector());
        convertedRow.add(convertedFieldValue);
      }
      convertedVal = convertedRow;
      break;
    default:
      throw new IllegalArgumentException("Cannot convert type " + oiCategory);
    }

    return convertedVal;
  }

  static void setRowFromStruct(Row row, Object structVal, StructObjectInspector soi) {
    Schema structSchema = row.getSchema();
    // Add struct field data to the Row
    List<? extends StructField> structFields = soi.getAllStructFieldRefs();
    for (int idx = 0; idx < structFields.size(); ++idx) {
      StructField structField = structFields.get(idx);

      Object convertedFieldValue = convertValue(
          soi.getStructFieldData(structVal, structField),
          structField.getFieldObjectInspector());
      row.setValue(idx, convertedFieldValue);
    }
  }

  protected AbstractSerDe initSerDe(Configuration conf) throws SerDeException {
    Properties props = new Properties();
    StringBuilder columnsBuffer = new StringBuilder();
    StringBuilder typesBuffer = new StringBuilder();
    boolean isFirst = true;
    for (FieldDesc colDesc : schema.getColumns()) {
      if (!isFirst) {
        columnsBuffer.append(',');
        typesBuffer.append(',');
      }
      columnsBuffer.append(colDesc.getName());
      typesBuffer.append(colDesc.getTypeInfo().toString());
      isFirst = false;
    }
    String columns = columnsBuffer.toString();
    String types = typesBuffer.toString();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    props.put(serdeConstants.ESCAPE_CHAR, "\\");
    AbstractSerDe serde = new LazyBinarySerDe();
    serde.initialize(conf, props);

    return serde;
  }
}
