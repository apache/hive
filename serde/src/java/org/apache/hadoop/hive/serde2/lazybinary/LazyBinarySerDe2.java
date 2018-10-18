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

package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Subclass of LazyBinarySerDe with faster serialization, initializing a serializer based on the
 * row columns rather than checking the ObjectInspector category/primitiveType for every value.
 * This appears to be around 3x faster than the LazyBinarSerDe serialization.
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class LazyBinarySerDe2 extends LazyBinarySerDe {
  LBSerializer rowSerializer;

  public LazyBinarySerDe2() throws SerDeException {
    super();
  }

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    super.initialize(conf, tbl);
    ObjectInspector oi = getObjectInspector();

    rowSerializer = createLBSerializer(oi);
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // serialize the row as BytesWritable
    serializeByteStream.reset();
    rowSerializer.serializeValue(serializeByteStream, obj, objInspector, true, nullMapKey);
    serializeBytesWritable.set(serializeByteStream.getData(), 0, serializeByteStream.getLength());

    // Stats bookkeeping
    serializedSize = serializeByteStream.getLength();
    lastOperationSerialize = true;
    lastOperationDeserialize = false;

    return serializeBytesWritable;
  }

  /**
   * Generate a LBSerializer for the given primitive ObjectInspector
   * @param poi
   * @return
   */
  LBSerializer createPrimitiveLBSerializer(PrimitiveObjectInspector poi) {
    switch (poi.getPrimitiveCategory()) {
    case VOID:
      return new LBVoidSerializer();
    case BOOLEAN:
      return new LBBooleanSerializer();
    case BYTE:
      return new LBByteSerializer();
    case SHORT:
      return new LBShortSerializer();
    case INT:
      return new LBIntSerializer();
    case LONG:
      return new LBLongSerializer();
    case FLOAT:
      return new LBFloatSerializer();
    case DOUBLE:
      return new LBDoubleSerializer();
    case STRING:
      return new LBStringSerializer();
    case CHAR:
      return new LBHiveCharSerializer();
    case VARCHAR:
      return new LBHiveVarcharSerializer();
    case BINARY:
      return new LBBinarySerializer();
    case DATE:
      return new LBDateSerializer();
    case TIMESTAMP:
      return new LBTimestampSerializer();
    case INTERVAL_YEAR_MONTH:
      return new LBHiveIntervalYearMonthSerializer();
    case INTERVAL_DAY_TIME:
      return new LBHiveIntervalDayTimeSerializer();
    case DECIMAL:
      return new LBHiveDecimalSerializer();
    default:
      throw new IllegalArgumentException("Unsupported primitive category " + poi.getPrimitiveCategory());
    }
  }

  /**
   * Generate a LBSerializer for the given ObjectInspector
   * @param oi
   * @return
   */
  LBSerializer createLBSerializer(ObjectInspector oi) {
    switch (oi.getCategory()) {
    case PRIMITIVE:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      return createPrimitiveLBSerializer(poi);
    case LIST:
      ListObjectInspector loi = (ListObjectInspector) oi;
      ObjectInspector eoi = loi.getListElementObjectInspector();
      return new LBListSerializer(createLBSerializer(eoi));
    case MAP:
      MapObjectInspector moi = (MapObjectInspector) oi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();
      return new LBMapSerializer(createLBSerializer(koi), createLBSerializer(voi));
    case STRUCT:
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      LBSerializer[] fieldSerializers = new LBSerializer[fields.size()];
      for (int idx = 0; idx < fieldSerializers.length; ++idx) {
        fieldSerializers[idx] = createLBSerializer(fields.get(idx).getFieldObjectInspector());
      }
      return new LBStructSerializer(fieldSerializers);
    case UNION:
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      List<ObjectInspector> unionFields = uoi.getObjectInspectors();
      LBSerializer[] unionFieldSerializers = new LBSerializer[unionFields.size()];
      for (int idx = 0; idx < unionFieldSerializers.length; ++idx) {
        unionFieldSerializers[idx] = createLBSerializer(unionFields.get(idx));
      }
      return new LBUnionSerializer(unionFieldSerializers);
    default:
      throw new IllegalArgumentException("Unsupported category " + oi.getCategory());
    }
  }

  /**
   * Abstract serializer class for serializing to LazyBinary format.
   */
  abstract static class LBSerializer {
    public void serializeValue(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      if (obj != null) {
        serialize(byteStream, obj, objInspector, skipLengthPrefix, warnedOnceNullMapKey);
      }
    }

    abstract void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey);
  }

  static class LBVoidSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      return;
    }
  }

  static class LBBooleanSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      boolean v = ((BooleanObjectInspector) objInspector).get(obj);
      byteStream.write((byte) (v ? 1 : 0));
    }
  }

  static class LBByteSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      ByteObjectInspector boi = (ByteObjectInspector) objInspector;
      byte v = boi.get(obj);
      byteStream.write(v);
    }
  }

  static class LBShortSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      ShortObjectInspector spoi = (ShortObjectInspector) objInspector;
      short v = spoi.get(obj);
      byteStream.write((byte) (v >> 8));
      byteStream.write((byte) (v));
    }
  }

  static class LBIntSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      IntObjectInspector ioi = (IntObjectInspector) objInspector;
      int v = ioi.get(obj);
      LazyBinaryUtils.writeVInt(byteStream, v);
    }
  }

  static class LBLongSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      LongObjectInspector loi = (LongObjectInspector) objInspector;
      long v = loi.get(obj);
      LazyBinaryUtils.writeVLong(byteStream, v);
      return;
    }
  }

  static class LBFloatSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      FloatObjectInspector foi = (FloatObjectInspector) objInspector;
      int v = Float.floatToIntBits(foi.get(obj));
      byteStream.write((byte) (v >> 24));
      byteStream.write((byte) (v >> 16));
      byteStream.write((byte) (v >> 8));
      byteStream.write((byte) (v));
    }
  }

  static class LBDoubleSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      DoubleObjectInspector doi = (DoubleObjectInspector) objInspector;
      LazyBinaryUtils.writeDouble(byteStream, doi.get(obj));
    }
  }

  static class LBStringSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      StringObjectInspector soi = (StringObjectInspector) objInspector;
      Text t = soi.getPrimitiveWritableObject(obj);
      serializeText(byteStream, t, skipLengthPrefix);
    }
  }

  static class LBHiveCharSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      HiveCharObjectInspector hcoi = (HiveCharObjectInspector) objInspector;
      Text t = hcoi.getPrimitiveWritableObject(obj).getTextValue();
      serializeText(byteStream, t, skipLengthPrefix);
    }
  }

  static class LBHiveVarcharSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      HiveVarcharObjectInspector hcoi = (HiveVarcharObjectInspector) objInspector;
      Text t = hcoi.getPrimitiveWritableObject(obj).getTextValue();
      serializeText(byteStream, t, skipLengthPrefix);
    }
  }

  static class LBBinarySerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      BinaryObjectInspector baoi = (BinaryObjectInspector) objInspector;
      BytesWritable bw = baoi.getPrimitiveWritableObject(obj);
      int length = bw.getLength();
      if(!skipLengthPrefix){
        LazyBinaryUtils.writeVInt(byteStream, length);
      } else {
        if (length == 0){
          throw new RuntimeException("LazyBinaryColumnarSerde cannot serialize a non-null zero "
              + "length binary field. Consider using either LazyBinarySerde or ColumnarSerde.");
        }
      }
      byteStream.write(bw.getBytes(),0,length);
    }
  }

  static class LBDateSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      DateWritableV2 d = ((DateObjectInspector) objInspector).getPrimitiveWritableObject(obj);
      LazyBinarySerDe.writeDateToByteStream(byteStream, d);
    }
  }

  static class LBTimestampSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      TimestampObjectInspector toi = (TimestampObjectInspector) objInspector;
      TimestampWritableV2 t = toi.getPrimitiveWritableObject(obj);
      t.writeToByteStream(byteStream);
    }
  }

  static class LBHiveIntervalYearMonthSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      HiveIntervalYearMonthWritable intervalYearMonth =
          ((HiveIntervalYearMonthObjectInspector) objInspector).getPrimitiveWritableObject(obj);
      intervalYearMonth.writeToByteStream(byteStream);
    }
  }

  static class LBHiveIntervalDayTimeSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      HiveIntervalDayTimeWritable intervalDayTime =
          ((HiveIntervalDayTimeObjectInspector) objInspector).getPrimitiveWritableObject(obj);
      intervalDayTime.writeToByteStream(byteStream);
    }
  }

  static class LBHiveDecimalSerializer extends LBSerializer {
    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      HiveDecimalObjectInspector bdoi = (HiveDecimalObjectInspector) objInspector;
      HiveDecimalWritable t = bdoi.getPrimitiveWritableObject(obj);
      if (t == null) {
        return;
      }
      writeToByteStream(byteStream, t);
    }
  }

  static class LBListSerializer extends LBSerializer {
    LBSerializer elementSerializer;

    public LBListSerializer(LBSerializer elementSerializer) {
      super();
      this.elementSerializer = elementSerializer;
    }

    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      ListObjectInspector loi = (ListObjectInspector) objInspector;
      ObjectInspector eoi = loi.getListElementObjectInspector();

      int byteSizeStart = 0;
      int listStart = 0;
      if (!skipLengthPrefix) {
        // 1/ reserve spaces for the byte size of the list
        // which is a integer and takes four bytes
        byteSizeStart = byteStream.getLength();
        byteStream.reserve(4);
        listStart = byteStream.getLength();
      }
      // 2/ write the size of the list as a VInt
      int size = loi.getListLength(obj);
      LazyBinaryUtils.writeVInt(byteStream, size);

      // 3/ write the null bytes
      byte nullByte = 0;
      for (int eid = 0; eid < size; eid++) {
        // set the bit to 1 if an element is not null
        if (null != loi.getListElement(obj, eid)) {
          nullByte |= 1 << (eid % 8);
        }
        // store the byte every eight elements or
        // if this is the last element
        if (7 == eid % 8 || eid == size - 1) {
          byteStream.write(nullByte);
          nullByte = 0;
        }
      }

      // 4/ write element by element from the list
      for (int eid = 0; eid < size; eid++) {
        elementSerializer.serializeValue(
            byteStream, loi.getListElement(obj, eid), eoi,
            false, warnedOnceNullMapKey);
      }

      if (!skipLengthPrefix) {
        // 5/ update the list byte size
        int listEnd = byteStream.getLength();
        int listSize = listEnd - listStart;
        writeSizeAtOffset(byteStream, byteSizeStart, listSize);
      }
      return;
    }
  }

  static class LBMapSerializer extends LBSerializer {
    LBSerializer keySerializer;
    LBSerializer valSerializer;

    public LBMapSerializer(LBSerializer keySerializer,
        LBSerializer valSerializer) {
      super();
      this.keySerializer = keySerializer;
      this.valSerializer = valSerializer;
    }

    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      MapObjectInspector moi = (MapObjectInspector) objInspector;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();
      Map<?, ?> map = moi.getMap(obj);

      int byteSizeStart = 0;
      int mapStart = 0;
      if (!skipLengthPrefix) {
        // 1/ reserve spaces for the byte size of the map
        // which is a integer and takes four bytes
        byteSizeStart = byteStream.getLength();
        byteStream.reserve(4);
        mapStart = byteStream.getLength();
      }

      // 2/ write the size of the map which is a VInt
      int size = map.size();
      LazyBinaryUtils.writeVInt(byteStream, size);

      // 3/ write the null bytes
      int b = 0;
      byte nullByte = 0;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        // set the bit to 1 if a key is not null
        if (null != entry.getKey()) {
          nullByte |= 1 << (b % 8);
        } else if (warnedOnceNullMapKey != null) {
          if (!warnedOnceNullMapKey.value) {
            LOG.warn("Null map key encountered! Ignoring similar problems.");
          }
          warnedOnceNullMapKey.value = true;
        }
        b++;
        // set the bit to 1 if a value is not null
        if (null != entry.getValue()) {
          nullByte |= 1 << (b % 8);
        }
        b++;
        // write the byte to stream every 4 key-value pairs
        // or if this is the last key-value pair
        if (0 == b % 8 || b == size * 2) {
          byteStream.write(nullByte);
          nullByte = 0;
        }
      }

      // 4/ write key-value pairs one by one
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        keySerializer.serializeValue(byteStream, entry.getKey(), koi, false, warnedOnceNullMapKey);
        valSerializer.serializeValue(byteStream, entry.getValue(), voi, false, warnedOnceNullMapKey);
      }

      if (!skipLengthPrefix) {
        // 5/ update the byte size of the map
        int mapEnd = byteStream.getLength();
        int mapSize = mapEnd - mapStart;
        writeSizeAtOffset(byteStream, byteSizeStart, mapSize);
      }
      return;
    }
  }

  static class LBStructSerializer extends LBSerializer {
    LBSerializer[] serializers;
    Object[] fieldData;

    public LBStructSerializer(LBSerializer[] serializers) {
      super();
      this.serializers = serializers;
      this.fieldData = new Object[serializers.length];
    }      

    @Override
    void serialize(RandomAccessOutput byteStream, Object obj, ObjectInspector objInspector,
        boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey) {
      int lasti = 0;
      byte nullByte = 0;
      int size = serializers.length;

      int byteSizeStart = 0;
      int typeStart = 0;
      if (!skipLengthPrefix) {
        // 1/ reserve spaces for the byte size of the struct
        // which is a integer and takes four bytes
        byteSizeStart = byteStream.getLength();
        byteStream.reserve(4);
        typeStart = byteStream.getLength();
      }

      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      for (int i = 0; i < size; ++i) {
        StructField structField = fields.get(i);
        fieldData[i] = soi.getStructFieldData(obj, structField);
      }

      for (int i = 0; i < size; ++i) {
        // set bit to 1 if a field is not null
        if (null != fieldData[i]) {
          nullByte |= 1 << (i % 8);
        }
        // write the null byte every eight elements or
        // if this is the last element and serialize the
        // corresponding 8 struct fields at the same time
        if (7 == i % 8 || i == size - 1) {
          byteStream.write(nullByte);
          for (int j = lasti; j <= i; j++) {
            serializers[j].serializeValue(
                byteStream, fieldData[j], fields.get(j).getFieldObjectInspector(),
                false, warnedOnceNullMapKey);
          }
          lasti = i + 1;
          nullByte = 0;
        }
      }

      if (!skipLengthPrefix) {
        // 3/ update the byte size of the struct
        int typeEnd = byteStream.getLength();
        int typeSize = typeEnd - typeStart;
        writeSizeAtOffset(byteStream, byteSizeStart, typeSize);
      }
    }
  }

  static class LBUnionSerializer extends LBSerializer {
    LBSerializer[] unionFieldSerializers;

    public LBUnionSerializer(LBSerializer[] unionFieldSerializers) {
      this.unionFieldSerializers = unionFieldSerializers;
    }

    @Override
    void serialize(RandomAccessOutput byteStream, Object obj,
        ObjectInspector objInspector, boolean skipLengthPrefix,
        BooleanRef warnedOnceNullMapKey) {
      int byteSizeStart = 0;
      int typeStart = 0;
      if (!skipLengthPrefix) {
        // 1/ reserve spaces for the byte size of the struct
        // which is a integer and takes four bytes
        byteSizeStart = byteStream.getLength();
        byteStream.reserve(4);
        typeStart = byteStream.getLength();
      }

      // 2/ serialize the union - tag/value
      UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
      byte tag = uoi.getTag(obj);
      byteStream.write(tag);
      unionFieldSerializers[tag].serializeValue(
          byteStream, uoi.getField(obj), uoi.getObjectInspectors().get(tag),
          false, warnedOnceNullMapKey);

      if (!skipLengthPrefix) {
        // 3/ update the byte size of the struct
        int typeEnd = byteStream.getLength();
        int typeSize = typeEnd - typeStart;
        writeSizeAtOffset(byteStream, byteSizeStart, typeSize);
      }
    }
  }
}
