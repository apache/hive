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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * The base class for MapJoinKey.
 * Ideally, this should now be removed, some table wrappers have no key object.
 */
public abstract class MapJoinKey {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public abstract void write(MapJoinObjectSerDeContext context, ObjectOutputStream out)
      throws IOException, SerDeException;

  public abstract boolean hasAnyNulls(int fieldCount, boolean[] nullsafes);

  @SuppressWarnings("deprecation")
  public static MapJoinKey read(Output output, MapJoinObjectSerDeContext context,
      Writable writable) throws SerDeException, HiveException {
    AbstractSerDe serde = context.getSerDe();
    Object obj = serde.deserialize(writable);
    MapJoinKeyObject result = new MapJoinKeyObject();
    result.read(serde.getObjectInspector(), obj);
    return result;
  }

  private static final HashSet<PrimitiveCategory> SUPPORTED_PRIMITIVES
      = new HashSet<PrimitiveCategory>();
  static {
    // All but decimal.
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.BOOLEAN);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.VOID);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.BOOLEAN);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.BYTE);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.SHORT);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.INT);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.LONG);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.FLOAT);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.DOUBLE);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.STRING);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.DATE);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.TIMESTAMP);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.INTERVAL_YEAR_MONTH);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.INTERVAL_DAY_TIME);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.BINARY);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.VARCHAR);
    SUPPORTED_PRIMITIVES.add(PrimitiveCategory.CHAR);
  }

  public static boolean isSupportedField(ObjectInspector foi) {
    if (foi.getCategory() != Category.PRIMITIVE) return false; // not supported
    PrimitiveCategory pc = ((PrimitiveObjectInspector)foi).getPrimitiveCategory();
    if (!SUPPORTED_PRIMITIVES.contains(pc)) return false; // not supported
    return true;
  }

  public static boolean isSupportedField(TypeInfo typeInfo) {
    if (typeInfo.getCategory() != Category.PRIMITIVE) return false; // not supported
    PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
    PrimitiveCategory pc = primitiveTypeInfo.getPrimitiveCategory();
    if (!SUPPORTED_PRIMITIVES.contains(pc)) return false; // not supported
    return true;
  }

  public static boolean isSupportedField(String typeName) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
    return isSupportedField(typeInfo);
  }


  public static MapJoinKey readFromVector(Output output, MapJoinKey key, Object[] keyObject,
      List<ObjectInspector> keyOIs, boolean mayReuseKey) throws HiveException {
    MapJoinKeyObject result = mayReuseKey ? (MapJoinKeyObject)key : new MapJoinKeyObject();
    result.setKeyObjects(keyObject);
    return result;
  }

  /**
   * Serializes row to output for vectorized path.
   * @param byteStream Output to reuse. Can be null, in that case a new one would be created.
   */
  public static Output serializeVector(Output byteStream, VectorHashKeyWrapperBase kw,
      VectorExpressionWriter[] keyOutputWriters, VectorHashKeyWrapperBatch keyWrapperBatch,
      boolean[] nulls, boolean[] sortableSortOrders, byte[] nullMarkers, byte[] notNullMarkers)
              throws HiveException, SerDeException {
    Object[] fieldData = new Object[keyOutputWriters.length];
    List<ObjectInspector> fieldOis = new ArrayList<ObjectInspector>();
    for (int i = 0; i < keyOutputWriters.length; ++i) {
      VectorExpressionWriter writer = keyOutputWriters[i];
      fieldOis.add(writer.getObjectInspector());
      // This is rather convoluted... to simplify for perf, we could call getRawKeyValue
      // instead of writable, and serialize based on Java type as opposed to OI.
      fieldData[i] = keyWrapperBatch.getWritableKeyValue(kw, i, writer);
      if (nulls != null) {
        nulls[i] = (fieldData[i] == null);
      }
    }
    return serializeRow(byteStream, fieldData, fieldOis, sortableSortOrders,
            nullMarkers, notNullMarkers);
  }

  public static MapJoinKey readFromRow(Output output, MapJoinKey key, Object[] keyObject,
      List<ObjectInspector> keyFieldsOI, boolean mayReuseKey) throws HiveException {
    MapJoinKeyObject result = mayReuseKey ? (MapJoinKeyObject)key : new MapJoinKeyObject();
    result.readFromRow(keyObject, keyFieldsOI);
    return result;
  }

  /**
   * Serializes row to output.
   * @param byteStream Output to reuse. Can be null, in that case a new one would be created.
   */
  public static Output serializeRow(Output byteStream, Object[] fieldData,
      List<ObjectInspector> fieldOis, boolean[] sortableSortOrders,
      byte[] nullMarkers, byte[] notNullMarkers) throws HiveException {
    if (byteStream == null) {
      byteStream = new Output();
    } else {
      byteStream.reset();
    }
    try {
      if (fieldData.length == 0) {
        byteStream.reset();
      } else if (sortableSortOrders == null) {
        LazyBinarySerDe.serializeStruct(byteStream, fieldData, fieldOis);
      } else {
        BinarySortableSerDe.serializeStruct(byteStream, fieldData, fieldOis, sortableSortOrders,
                nullMarkers, notNullMarkers);
      }
    } catch (SerDeException e) {
      throw new HiveException("Serialization error", e);
    }
    return byteStream;
  }
}
