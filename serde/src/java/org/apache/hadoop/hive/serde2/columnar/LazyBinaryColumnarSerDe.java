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
package org.apache.hadoop.hive.serde2.columnar;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;


/**
 * LazyBinaryColumnarSerDe. This serde combines elements of columnar serde and lazybinary serde
 * to produce a serde which serializes columns into a BytesRefArrayWritable in a compact binary
 * format and which is deserialized in a lazy, i.e. on-demand fashion.
 *
 */
public class LazyBinaryColumnarSerDe extends ColumnarSerDeBase {

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + columnNames
        + ":"
        + columnTypes + "]";
  }

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    SerDeParameters serdeParams = new SerDeParameters();
    LazyUtils.extractColumnInfo(tbl, serdeParams, getClass().getName());
    columnNames = serdeParams.getColumnNames();
    columnTypes = serdeParams.getColumnTypes();

    cachedObjectInspector = LazyBinaryFactory.createColumnarStructInspector(
        columnNames, columnTypes);
    java.util.ArrayList<Integer> notSkipIDs = ColumnProjectionUtils.getReadColumnIDs(conf);
    cachedLazyStruct = new LazyBinaryColumnarStruct(cachedObjectInspector, notSkipIDs);
    int size = columnTypes.size();
    super.initialize(size);
  }

  static final byte[] INVALID_UTF__SINGLE_BYTE = {(byte)Integer.parseInt("10111111", 2)};
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);

    boolean warnedOnceNullMapKey = false;
    serializeStream.reset();
    serializedSize = 0;
    int streamOffset = 0;
    // Serialize each field
    for (int i = 0; i < fields.size(); i++) {
      // Get the field objectInspector and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      Object f = (list == null ? null : list.get(i));
      //empty strings are marked by an invalid utf single byte sequence. A valid utf stream cannot
      //produce this sequence
      if ((f != null) && (foi.getCategory().equals(ObjectInspector.Category.PRIMITIVE))
          && ((PrimitiveObjectInspector) foi).getPrimitiveCategory().equals(
              PrimitiveObjectInspector.PrimitiveCategory.STRING)
          && ((StringObjectInspector) foi).getPrimitiveJavaObject(f).length() == 0) {
        serializeStream.write(INVALID_UTF__SINGLE_BYTE, 0, 1);
      } else {
        LazyBinarySerDe.serialize(serializeStream, f, foi, true, warnedOnceNullMapKey);
      }
      field[i].set(serializeStream.getData(), streamOffset, serializeStream
          .getCount()
          - streamOffset);
      streamOffset = serializeStream.getCount();
    }
    serializedSize = serializeStream.getCount();
    lastOperationSerialize = true;
    lastOperationDeserialize = false;
    return serializeCache;
  }
}
