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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * The LazyBinarySerDe class combines the lazy property of LazySimpleSerDe class
 * and the binary property of BinarySortable class. Lazy means a field is not
 * deserialized until required. Binary means a field is serialized in binary
 * compact format.
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class LazyBinarySerDe extends AbstractSerDe {
  public static final Logger LOG = LoggerFactory.getLogger(LazyBinarySerDe.class.getName());

  public LazyBinarySerDe() throws SerDeException {
  }

  List<String> columnNames;
  List<TypeInfo> columnTypes;

  TypeInfo rowTypeInfo;
  ObjectInspector cachedObjectInspector;

  // The object for storing row data
  LazyBinaryStruct cachedLazyBinaryStruct;

  int serializedSize;
  SerDeStats stats;
  boolean lastOperationSerialize;
  boolean lastOperationDeserialize;

  /**
   * Initialize the SerDe with configuration and table information.
   */
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
    }
    assert (columnNames.size() == columnTypes.size());
    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    // Create the object inspector and the lazy binary struct object
    cachedObjectInspector = LazyBinaryUtils
        .getLazyBinaryObjectInspectorFromTypeInfo(rowTypeInfo);
    cachedLazyBinaryStruct = (LazyBinaryStruct) LazyBinaryFactory
        .createLazyBinaryObject(cachedObjectInspector);
    // output debug info
    LOG.debug("LazyBinarySerDe initialized with: columnNames=" + columnNames
        + " columnTypes=" + columnTypes);

    serializedSize = 0;
    stats = new SerDeStats();
    lastOperationSerialize = false;
    lastOperationDeserialize = false;

  }

  /**
   * Returns the ObjectInspector for the row.
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  /**
   * Returns the Writable Class after serialization.
   */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  // The wrapper for byte array
  ByteArrayRef byteArrayRef;

  /**
   * Deserialize a table record to a lazybinary struct.
   */
  @Override
  public Object deserialize(Writable field) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }
    BinaryComparable b = (BinaryComparable) field;
    if (b.getLength() == 0) {
      return null;
    }
    byteArrayRef.setData(b.getBytes());
    cachedLazyBinaryStruct.init(byteArrayRef, 0, b.getLength());
    lastOperationSerialize = false;
    lastOperationDeserialize = true;
    return cachedLazyBinaryStruct;
  }

  /**
   * The reusable output buffer and serialize byte buffer.
   */
  BytesWritable serializeBytesWritable = new BytesWritable();
  ByteStream.Output serializeByteStream = new ByteStream.Output();
  BooleanRef nullMapKey = new BooleanRef(false);

  /**
   * Serialize an object to a byte buffer in a binary compact way.
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    // make sure it is a struct record
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    serializeByteStream.reset();
    // serialize the row as a struct
    serializeStruct(serializeByteStream, obj, (StructObjectInspector) objInspector, nullMapKey);
    // return the serialized bytes
    serializeBytesWritable.set(serializeByteStream.getData(), 0, serializeByteStream.getLength());

    serializedSize = serializeByteStream.getLength();
    lastOperationSerialize = true;
    lastOperationDeserialize = false;
    return serializeBytesWritable;
  }

  public static class StringWrapper {
    public byte[] bytes;
    public int start, length;

    public void set(byte[] bytes, int start, int length) {
      this.bytes = bytes;
      this.start = start;
      this.length = length;
    }
  }

  private static void serializeStruct(RandomAccessOutput byteStream, Object obj,
      StructObjectInspector soi, BooleanRef warnedOnceNullMapKey) throws SerDeException {
    // do nothing for null struct
    if (null == obj) {
      return;
    }

    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    int size = fields.size();
    Object[] fieldData = new Object[size];
    List<ObjectInspector> fieldOis = new ArrayList<ObjectInspector>(size);
    for (int i = 0; i < size; ++i) {
      StructField field = fields.get(i);
      fieldData[i] = soi.getStructFieldData(obj, field);
      fieldOis.add(field.getFieldObjectInspector());
    }

    serializeStruct(byteStream, fieldData, fieldOis, warnedOnceNullMapKey);
  }

  public static void serializeStruct(RandomAccessOutput byteStream, Object[] fieldData,
      List<ObjectInspector> fieldOis) throws SerDeException {
    serializeStruct(byteStream, fieldData, fieldOis, null);
  }

  /**
   * Serialize a struct object without writing the byte size. This function is
   * shared by both row serialization and struct serialization.
   *
   * @param byteStream
   *          the byte stream storing the serialization data
   * @param fieldData
   *          the struct object to serialize
   * @param fieldOis
   *          the struct object inspector
   * @param warnedOnceNullMapKey a boolean indicating whether a warning
   *          has been issued once already when encountering null map keys
   */
  private static void serializeStruct(RandomAccessOutput byteStream, Object[] fieldData,
      List<ObjectInspector> fieldOis, BooleanRef warnedOnceNullMapKey)
          throws SerDeException {

    int lasti = 0;
    byte nullByte = 0;
    int size = fieldData.length;

    for (int i = 0; i < size; i++) {
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
          serialize(byteStream, fieldData[j], fieldOis.get(j), false, warnedOnceNullMapKey);
        }
        lasti = i + 1;
        nullByte = 0;
      }
    }
  }

  private static void serializeUnion(RandomAccessOutput byteStream, Object obj,
    UnionObjectInspector uoi, BooleanRef warnedOnceNullMapKey) throws SerDeException {
    byte tag = uoi.getTag(obj);
    byteStream.write(tag);
    serialize(byteStream, uoi.getField(obj), uoi.getObjectInspectors().get(tag), false, warnedOnceNullMapKey);
  }

  protected static void serializeText(
      RandomAccessOutput byteStream, Text t, boolean skipLengthPrefix) {
    /* write byte size of the string which is a vint */
    int length = t.getLength();
    if (!skipLengthPrefix) {
      LazyBinaryUtils.writeVInt(byteStream, length);
    }
    /* write string itself */
    byte[] data = t.getBytes();
    byteStream.write(data, 0, length);
  }

  public static class BooleanRef {
    public BooleanRef(boolean v) {
      value = v;
    }

    public boolean value;
  }

  public static void writeDateToByteStream(RandomAccessOutput byteStream,
                                            DateWritable date) {
    LazyBinaryUtils.writeVInt(byteStream, date.getDays());
  }

  public static void setFromBigIntegerBytesAndScale(byte[] bytes, int offset, int length,
                                  HiveDecimalWritable dec) {
    LazyBinaryUtils.VInt vInt = new LazyBinaryUtils.VInt();
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    int scale = vInt.value;
    offset += vInt.length;
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    offset += vInt.length;
    dec.setFromBigIntegerBytesAndScale(bytes, offset, vInt.value, scale);
  }

  public static void writeToByteStream(RandomAccessOutput byteStream,
                                       HiveDecimalWritable decWritable) {
    LazyBinaryUtils.writeVInt(byteStream, decWritable.scale());

    // NOTE: This writes into a scratch buffer within HiveDecimalWritable.
    //
    int byteLength = decWritable.bigIntegerBytesInternalScratch();

    LazyBinaryUtils.writeVInt(byteStream, byteLength);
    byteStream.write(decWritable.bigIntegerBytesInternalScratchBuffer(), 0, byteLength);
  }

  /**
   *
   * Allocate scratchLongs with HiveDecimal.SCRATCH_LONGS_LEN longs.
   * And, allocate scratch buffer with HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES bytes.
   *
   * @param byteStream
   * @param dec
   * @param scratchLongs
   * @param scratchBytes
   */
  public static void writeToByteStream(
      RandomAccessOutput byteStream,
      HiveDecimal dec,
      long[] scratchLongs, byte[] scratchBytes) {
    LazyBinaryUtils.writeVInt(byteStream, dec.scale());

    // Convert decimal into the scratch buffer without allocating a byte[] each time
    // for better performance.
    int byteLength = 
        dec.bigIntegerBytes(
            scratchLongs, scratchBytes);
    if (byteLength == 0) {
      throw new RuntimeException("Decimal to binary conversion failed");
    }
    LazyBinaryUtils.writeVInt(byteStream, byteLength);
    byteStream.write(scratchBytes, 0, byteLength);
  }

  /**
  *
  * Allocate scratchLongs with HiveDecimal.SCRATCH_LONGS_LEN longs.
  * And, allocate scratch buffer with HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES bytes.
  *
  * @param byteStream
  * @param decWritable
  * @param scratchLongs
  * @param scratchBytes
  */
  public static void writeToByteStream(
      RandomAccessOutput byteStream,
      HiveDecimalWritable decWritable,
      long[] scratchLongs, byte[] scratchBytes) {
    LazyBinaryUtils.writeVInt(byteStream, decWritable.scale());
    int byteLength =
        decWritable.bigIntegerBytes(
            scratchLongs, scratchBytes);
    LazyBinaryUtils.writeVInt(byteStream, byteLength);
    byteStream.write(scratchBytes, 0, byteLength);
  }

  /**
   * A recursive function that serialize an object to a byte buffer based on its
   * object inspector.
   *
   * @param byteStream
   *          the byte stream storing the serialization data
   * @param obj
   *          the object to serialize
   * @param objInspector
   *          the object inspector
   * @param skipLengthPrefix a boolean indicating whether length prefix is
   *          needed for list/map/struct
   * @param warnedOnceNullMapKey a boolean indicating whether a warning
   *          has been issued once already when encountering null map keys
   */
  public static void serialize(RandomAccessOutput byteStream, Object obj,
      ObjectInspector objInspector, boolean skipLengthPrefix, BooleanRef warnedOnceNullMapKey)
          throws SerDeException {

    // do nothing for null object
    if (null == obj) {
      return;
    }

    switch (objInspector.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) objInspector;
      switch (poi.getPrimitiveCategory()) {
      case VOID: {
        return;
      }
      case BOOLEAN: {
        boolean v = ((BooleanObjectInspector) poi).get(obj);
        byteStream.write((byte) (v ? 1 : 0));
        return;
      }
      case BYTE: {
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        byte v = boi.get(obj);
        byteStream.write(v);
        return;
      }
      case SHORT: {
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        short v = spoi.get(obj);
        byteStream.write((byte) (v >> 8));
        byteStream.write((byte) (v));
        return;
      }
      case INT: {
        IntObjectInspector ioi = (IntObjectInspector) poi;
        int v = ioi.get(obj);
        LazyBinaryUtils.writeVInt(byteStream, v);
        return;
      }
      case LONG: {
        LongObjectInspector loi = (LongObjectInspector) poi;
        long v = loi.get(obj);
        LazyBinaryUtils.writeVLong(byteStream, v);
        return;
      }
      case FLOAT: {
        FloatObjectInspector foi = (FloatObjectInspector) poi;
        int v = Float.floatToIntBits(foi.get(obj));
        byteStream.write((byte) (v >> 24));
        byteStream.write((byte) (v >> 16));
        byteStream.write((byte) (v >> 8));
        byteStream.write((byte) (v));
        return;
      }
      case DOUBLE: {
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        LazyBinaryUtils.writeDouble(byteStream, doi.get(obj));
        return;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector) poi;
        Text t = soi.getPrimitiveWritableObject(obj);
        serializeText(byteStream, t, skipLengthPrefix);
        return;
      }
      case CHAR: {
        HiveCharObjectInspector hcoi = (HiveCharObjectInspector) poi;
        Text t = hcoi.getPrimitiveWritableObject(obj).getTextValue();
        serializeText(byteStream, t, skipLengthPrefix);
        return;
      }
      case VARCHAR: {
        HiveVarcharObjectInspector hcoi = (HiveVarcharObjectInspector) poi;
        Text t = hcoi.getPrimitiveWritableObject(obj).getTextValue();
        serializeText(byteStream, t, skipLengthPrefix);
        return;
      }
      case BINARY: {
        BinaryObjectInspector baoi = (BinaryObjectInspector) poi;
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
        return;
      }

      case DATE: {
        DateWritable d = ((DateObjectInspector) poi).getPrimitiveWritableObject(obj);
        writeDateToByteStream(byteStream, d);
        return;
      }
      case TIMESTAMP: {
        TimestampObjectInspector toi = (TimestampObjectInspector) poi;
        TimestampWritable t = toi.getPrimitiveWritableObject(obj);
        t.writeToByteStream(byteStream);
        return;
      }
      case TIMESTAMPLOCALTZ: {
        TimestampLocalTZWritable t = ((TimestampLocalTZObjectInspector) poi).getPrimitiveWritableObject(obj);
        t.writeToByteStream(byteStream);
        return;
      }

      case INTERVAL_YEAR_MONTH: {
        HiveIntervalYearMonthWritable intervalYearMonth =
            ((HiveIntervalYearMonthObjectInspector) poi).getPrimitiveWritableObject(obj);
        intervalYearMonth.writeToByteStream(byteStream);
        return;
      }

      case INTERVAL_DAY_TIME: {
        HiveIntervalDayTimeWritable intervalDayTime =
            ((HiveIntervalDayTimeObjectInspector) poi).getPrimitiveWritableObject(obj);
        intervalDayTime.writeToByteStream(byteStream);
        return;
      }

      case DECIMAL: {
        HiveDecimalObjectInspector bdoi = (HiveDecimalObjectInspector) poi;
        HiveDecimalWritable t = bdoi.getPrimitiveWritableObject(obj);
        if (t == null) {
          return;
        }
        writeToByteStream(byteStream, t);
        return;
      }

      default: {
        throw new RuntimeException("Unrecognized type: "
            + poi.getPrimitiveCategory());
      }
      }
    }
    case LIST: {
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
        serialize(byteStream, loi.getListElement(obj, eid), eoi, false, warnedOnceNullMapKey);
      }

      if (!skipLengthPrefix) {
        // 5/ update the list byte size
        int listEnd = byteStream.getLength();
        int listSize = listEnd - listStart;
        writeSizeAtOffset(byteStream, byteSizeStart, listSize);
      }
      return;
    }
    case MAP: {
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
        serialize(byteStream, entry.getKey(), koi, false, warnedOnceNullMapKey);
        serialize(byteStream, entry.getValue(), voi, false, warnedOnceNullMapKey);
      }

      if (!skipLengthPrefix) {
        // 5/ update the byte size of the map
        int mapEnd = byteStream.getLength();
        int mapSize = mapEnd - mapStart;
        writeSizeAtOffset(byteStream, byteSizeStart, mapSize);
      }
      return;
    }
    case STRUCT:
    case UNION:{
      int byteSizeStart = 0;
      int typeStart = 0;
      if (!skipLengthPrefix) {
        // 1/ reserve spaces for the byte size of the struct
        // which is a integer and takes four bytes
        byteSizeStart = byteStream.getLength();
        byteStream.reserve(4);
        typeStart = byteStream.getLength();
      }

      if (ObjectInspector.Category.STRUCT.equals(objInspector.getCategory()) ) {
        // 2/ serialize the struct
        serializeStruct(byteStream, obj, (StructObjectInspector) objInspector, warnedOnceNullMapKey);
      } else {
        // 2/ serialize the union
        serializeUnion(byteStream, obj, (UnionObjectInspector) objInspector, warnedOnceNullMapKey);
      }

      if (!skipLengthPrefix) {
        // 3/ update the byte size of the struct
        int typeEnd = byteStream.getLength();
        int typeSize = typeEnd - typeStart;
        writeSizeAtOffset(byteStream, byteSizeStart, typeSize);
      }
      return;
    }
    default: {
      throw new RuntimeException("Unrecognized type: "
          + objInspector.getCategory());
    }
    }
  }

  protected static void writeSizeAtOffset(
      RandomAccessOutput byteStream, int byteSizeStart, int size) {
    byteStream.writeInt(byteSizeStart, size);
  }

  /**
   * Returns the statistics after (de)serialization)
   */
  @Override
  public SerDeStats getSerDeStats() {
    // must be different
    assert (lastOperationSerialize != lastOperationDeserialize);

    if (lastOperationSerialize) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(cachedLazyBinaryStruct.getRawDataSerializedSize());
    }
    return stats;

  }
}
