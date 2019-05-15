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

package org.apache.hadoop.hive.serde2.binarysortable;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimalV1;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BinarySortableSerDe can be used to write data in a way that the data can be
 * compared byte-by-byte with the same order.
 *
 * The data format: NULL: a single byte (\0 or \1, check below) NON-NULL Primitives:
 * ALWAYS prepend a single byte (\0 or \1), and then: Boolean: FALSE = \1, TRUE = \2
 * Byte: flip the sign-bit to make sure negative comes before positive Short: flip the
 * sign-bit to make sure negative comes before positive Int: flip the sign-bit to
 * make sure negative comes before positive Long: flip the sign-bit to make sure
 * negative comes before positive Double: flip the sign-bit for positive double,
 * and all bits for negative double values String: NULL-terminated UTF-8 string,
 * with NULL escaped to \1 \1, and \1 escaped to \1 \2 NON-NULL Complex Types:
 * ALWAYS prepend a single byte \1, and then: Struct: one field by one field.
 * List: \1 followed by each element, and \0 to terminate Map: \1 followed by
 * each key and then each value, and \0 to terminate
 *
 * This SerDe takes an additional parameter SERIALIZATION_SORT_ORDER which is a
 * string containing only "+" and "-". The length of the string should equal to
 * the number of fields in the top-level struct for serialization. "+" means the
 * field should be sorted ascendingly, and "-" means descendingly. The sub
 * fields in the same top-level field will have the same sort order.
 *
 * This SerDe takes an additional parameter SERIALIZATION_NULL_SORT_ORDER which is a
 * string containing only "a" and "z". The length of the string should equal to
 * the number of fields in the top-level struct for serialization. "a" means that
 * NULL should come first (thus, single byte is \0 for ascending order, \1
 * for descending order), while "z" means that NULL should come last (thus, single
 * byte is \1 for ascending order, \0 for descending order).
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.SERIALIZATION_SORT_ORDER, serdeConstants.SERIALIZATION_NULL_SORT_ORDER})
public class BinarySortableSerDe extends AbstractSerDe {

  public static final Logger LOG = LoggerFactory.getLogger(BinarySortableSerDe.class.getName());

  public static final byte ZERO = (byte) 0;
  public static final byte ONE = (byte) 1;

  List<String> columnNames;
  List<TypeInfo> columnTypes;

  TypeInfo rowTypeInfo;
  StructObjectInspector rowObjectInspector;

  boolean[] columnSortOrderIsDesc;
  byte[] columnNullMarker;
  byte[] columnNotNullMarker;

  public static Charset decimalCharSet = Charset.forName("US-ASCII");

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // Get column names and sort order
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
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
    rowObjectInspector = (StructObjectInspector) TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
    row = new ArrayList<Object>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      row.add(null);
    }

    // Get the sort order
    String columnSortOrder = tbl
        .getProperty(serdeConstants.SERIALIZATION_SORT_ORDER);
    columnSortOrderIsDesc = new boolean[columnNames.size()];
    for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
      columnSortOrderIsDesc[i] = (columnSortOrder != null && columnSortOrder
          .charAt(i) == '-');
    }

    // Null first/last
    String columnNullOrder = tbl
        .getProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER);
    columnNullMarker = new byte[columnNames.size()];
    columnNotNullMarker = new byte[columnNames.size()];
    for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
      if (columnSortOrderIsDesc[i]) {
        // Descending
        if (columnNullOrder != null && columnNullOrder.charAt(i) == 'a') {
          // Null first
          columnNullMarker[i] = ONE;
          columnNotNullMarker[i] = ZERO;
        } else {
          // Null last (default for descending order)
          columnNullMarker[i] = ZERO;
          columnNotNullMarker[i] = ONE;
        }
      } else {
        // Ascending
        if (columnNullOrder != null && columnNullOrder.charAt(i) == 'z') {
          // Null last
          columnNullMarker[i] = ONE;
          columnNotNullMarker[i] = ZERO;
        } else {
          // Null first (default for ascending order)
          columnNullMarker[i] = ZERO;
          columnNotNullMarker[i] = ONE;
        }
      }
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  ArrayList<Object> row;
  InputByteBuffer inputByteBuffer = new InputByteBuffer();

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable data = (BytesWritable) blob;
    inputByteBuffer.reset(data.getBytes(), 0, data.getLength());

    try {
      for (int i = 0; i < columnNames.size(); i++) {
        row.set(i, deserialize(inputByteBuffer, columnTypes.get(i),
            columnSortOrderIsDesc[i], columnNullMarker[i], columnNotNullMarker[i], row.get(i)));
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return row;
  }

  static Object deserialize(InputByteBuffer buffer, TypeInfo type,
      boolean invert, byte nullMarker, byte notNullMarker, Object reuse) throws IOException {

    // Is this field a null?
    byte isNull = buffer.read(invert);
    if (isNull == nullMarker) {
      return null;
    }
    assert (isNull == notNullMarker);

    switch (type.getCategory()) {
    case PRIMITIVE: {
      PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
      switch (ptype.getPrimitiveCategory()) {
      case VOID: {
        return null;
      }
      case BOOLEAN: {
        BooleanWritable r = reuse == null ? new BooleanWritable()
            : (BooleanWritable) reuse;
        byte b = buffer.read(invert);
        assert (b == 1 || b == 2);
        r.set(b == 2);
        return r;
      }
      case BYTE: {
        ByteWritable r = reuse == null ? new ByteWritable()
            : (ByteWritable) reuse;
        r.set((byte) (buffer.read(invert) ^ 0x80));
        return r;
      }
      case SHORT: {
        ShortWritable r = reuse == null ? new ShortWritable()
            : (ShortWritable) reuse;
        int v = buffer.read(invert) ^ 0x80;
        v = (v << 8) + (buffer.read(invert) & 0xff);
        r.set((short) v);
        return r;
      }
      case INT: {
        IntWritable r = reuse == null ? new IntWritable() : (IntWritable) reuse;
        r.set(deserializeInt(buffer, invert));
        return r;
      }
      case LONG: {
        LongWritable r = reuse == null ? new LongWritable()
            : (LongWritable) reuse;
        r.set(deserializeLong(buffer, invert));
        return r;
      }
      case FLOAT: {
        FloatWritable r = reuse == null ? new FloatWritable()
            : (FloatWritable) reuse;
        int v = 0;
        for (int i = 0; i < 4; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        if ((v & (1 << 31)) == 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1 << 31);
        }
        r.set(Float.intBitsToFloat(v));
        return r;
      }
      case DOUBLE: {
        DoubleWritable r = reuse == null ? new DoubleWritable()
            : (DoubleWritable) reuse;
        long v = 0;
        for (int i = 0; i < 8; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        if ((v & (1L << 63)) == 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1L << 63);
        }
        r.set(Double.longBitsToDouble(v));
        return r;
      }
      case STRING: {
        Text r = reuse == null ? new Text() : (Text) reuse;
        return deserializeText(buffer, invert, r);
      }

      case CHAR: {
        HiveCharWritable r =
            reuse == null ? new HiveCharWritable() : (HiveCharWritable) reuse;
        // Use internal text member to read value
        deserializeText(buffer, invert, r.getTextValue());
        r.enforceMaxLength(getCharacterMaxLength(type));
        return r;
      }

      case VARCHAR: {
        HiveVarcharWritable r =
            reuse == null ? new HiveVarcharWritable() : (HiveVarcharWritable) reuse;
            // Use HiveVarchar's internal Text member to read the value.
            deserializeText(buffer, invert, r.getTextValue());
            // If we cache helper data for deserialization we could avoid having
            // to call getVarcharMaxLength() on every deserialize call.
            r.enforceMaxLength(getCharacterMaxLength(type));
            return r;
      }

      case BINARY: {
        BytesWritable bw = new BytesWritable() ;
        // Get the actual length first
        int start = buffer.tell();
        int length = 0;
        do {
          byte b = buffer.read(invert);
          if (b == 0) {
            // end of string
            break;
          }
          if (b == 1) {
            // the last char is an escape char. read the actual char
            buffer.read(invert);
          }
          length++;
        } while (true);

        if (length == buffer.tell() - start) {
          // No escaping happened, so we are already done.
          bw.set(buffer.getData(), start, length);
        } else {
          // Escaping happened, we need to copy byte-by-byte.
          // 1. Set the length first.
          bw.set(buffer.getData(), start, length);
          // 2. Reset the pointer.
          buffer.seek(start);
          // 3. Copy the data.
          byte[] rdata = bw.getBytes();
          for (int i = 0; i < length; i++) {
            byte b = buffer.read(invert);
            if (b == 1) {
              // The last char is an escape char, read the actual char.
              // The serialization format escape \0 to \1, and \1 to \2,
              // to make sure the string is null-terminated.
              b = (byte) (buffer.read(invert) - 1);
            }
            rdata[i] = b;
          }
          // 4. Read the null terminator.
          byte b = buffer.read(invert);
          assert (b == 0);
        }
        return bw;
      }

      case DATE: {
        DateWritableV2 d = reuse == null ? new DateWritableV2()
            : (DateWritableV2) reuse;
        d.set(deserializeInt(buffer, invert));
        return d;
      }

      case TIMESTAMP:
        TimestampWritableV2 t = (reuse == null ? new TimestampWritableV2() :
            (TimestampWritableV2) reuse);
        byte[] bytes = new byte[TimestampWritableV2.BINARY_SORTABLE_LENGTH];

        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = buffer.read(invert);
        }
        t.setBinarySortable(bytes, 0);
        return t;
      case TIMESTAMPLOCALTZ:
        TimestampLocalTZWritable tstz = (reuse == null ? new TimestampLocalTZWritable() :
            (TimestampLocalTZWritable) reuse);
        byte[] data = new byte[TimestampLocalTZWritable.BINARY_SORTABLE_LENGTH];
        for (int i = 0; i < data.length; i++) {
          data[i] = buffer.read(invert);
        }
        // Across MR process boundary tz is normalized and stored in type
        // and is not carried in data for each row.
        tstz.fromBinarySortable(data, 0, ((TimestampLocalTZTypeInfo) type).timeZone());
        return tstz;
      case INTERVAL_YEAR_MONTH: {
        HiveIntervalYearMonthWritable i = reuse == null ? new HiveIntervalYearMonthWritable()
            : (HiveIntervalYearMonthWritable) reuse;
        i.set(deserializeInt(buffer, invert));
        return i;
      }

      case INTERVAL_DAY_TIME: {
        HiveIntervalDayTimeWritable i = reuse == null ? new HiveIntervalDayTimeWritable()
            : (HiveIntervalDayTimeWritable) reuse;
        long totalSecs = deserializeLong(buffer, invert);
        int nanos = deserializeInt(buffer, invert);
        i.set(totalSecs, nanos);
        return i;
      }

      case DECIMAL: {
        // See serialization of decimal for explanation (below)

        HiveDecimalWritable bdw = (reuse == null ? new HiveDecimalWritable() :
          (HiveDecimalWritable) reuse);

        int b = buffer.read(invert) - 1;
        assert (b == 1 || b == -1 || b == 0);
        boolean positive = b != -1;

        int factor = buffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          factor = (factor << 8) + (buffer.read(invert) & 0xff);
        }

        if (!positive) {
          factor = -factor;
        }

        int start = buffer.tell();
        int length = 0;

        do {
          b = buffer.read(positive ? invert : !invert);
          assert(b != 1);

          if (b == 0) {
            // end of digits
            break;
          }

          length++;
        } while (true);

        final byte[] decimalBuffer = new byte[length];

        buffer.seek(start);
        for (int i = 0; i < length; ++i) {
          decimalBuffer[i] = buffer.read(positive ? invert : !invert);
        }

        // read the null byte again
        buffer.read(positive ? invert : !invert);

        String digits = new String(decimalBuffer, 0, length, decimalCharSet);
        BigInteger bi = new BigInteger(digits);
        HiveDecimal bd = HiveDecimal.create(bi).scaleByPowerOfTen(factor-length);

        if (!positive) {
          bd = bd.negate();
        }

        bdw.set(bd);
        return bdw;
      }

      default: {
        throw new RuntimeException("Unrecognized type: "
            + ptype.getPrimitiveCategory());
      }
      }
    }

    case LIST: {
      ListTypeInfo ltype = (ListTypeInfo) type;
      TypeInfo etype = ltype.getListElementTypeInfo();

      // Create the list if needed
      ArrayList<Object> r = reuse == null ? new ArrayList<Object>()
          : (ArrayList<Object>) reuse;

      // Read the list
      int size = 0;
      while (true) {
        int more = buffer.read(invert);
        if (more == 0) {
          // \0 to terminate
          break;
        }
        // \1 followed by each element
        assert (more == 1);
        if (size == r.size()) {
          r.add(null);
        }
        r.set(size, deserialize(buffer, etype, invert, nullMarker, notNullMarker, r.get(size)));
        size++;
      }
      // Remove additional elements if the list is reused
      while (r.size() > size) {
        r.remove(r.size() - 1);
      }
      return r;
    }
    case MAP: {
      MapTypeInfo mtype = (MapTypeInfo) type;
      TypeInfo ktype = mtype.getMapKeyTypeInfo();
      TypeInfo vtype = mtype.getMapValueTypeInfo();

      // Create the map if needed
      Map<Object, Object> r;
      if (reuse == null || reuse.getClass() != LinkedHashMap.class) {
        r = new LinkedHashMap<Object, Object>();
      } else {
        r = (Map<Object, Object>) reuse;
        r.clear();
      }

      while (true) {
        int more = buffer.read(invert);
        if (more == 0) {
          // \0 to terminate
          break;
        }
        // \1 followed by each key and then each value
        assert (more == 1);
        Object k = deserialize(buffer, ktype, invert, nullMarker, notNullMarker, null);
        Object v = deserialize(buffer, vtype, invert, nullMarker, notNullMarker, null);
        r.put(k, v);
      }
      return r;
    }
    case STRUCT: {
      StructTypeInfo stype = (StructTypeInfo) type;
      List<TypeInfo> fieldTypes = stype.getAllStructFieldTypeInfos();
      int size = fieldTypes.size();
      // Create the struct if needed
      ArrayList<Object> r = reuse == null ? new ArrayList<Object>(size)
          : (ArrayList<Object>) reuse;
      assert (r.size() <= size);
      // Set the size of the struct
      while (r.size() < size) {
        r.add(null);
      }
      // Read one field by one field
      for (int eid = 0; eid < size; eid++) {
        r
            .set(eid, deserialize(buffer, fieldTypes.get(eid), invert, nullMarker, notNullMarker, r
            .get(eid)));
      }
      return r;
    }
    case UNION: {
      UnionTypeInfo utype = (UnionTypeInfo) type;
      StandardUnion r = reuse == null ? new StandardUnion()
          : (StandardUnion) reuse;
      // Read the tag
      byte tag = buffer.read(invert);
      r.setTag(tag);
      r.setObject(deserialize(buffer, utype.getAllUnionObjectTypeInfos().get(tag),
          invert, nullMarker, notNullMarker, null));
      return r;
    }
    default: {
      throw new RuntimeException("Unrecognized type: " + type.getCategory());
    }
    }
  }

  private static int deserializeInt(InputByteBuffer buffer, boolean invert) throws IOException {
    int v = buffer.read(invert) ^ 0x80;
    for (int i = 0; i < 3; i++) {
      v = (v << 8) + (buffer.read(invert) & 0xff);
    }
    return v;
  }

  private static long deserializeLong(InputByteBuffer buffer, boolean invert) throws IOException {
    long v = buffer.read(invert) ^ 0x80;
    for (int i = 0; i < 7; i++) {
      v = (v << 8) + (buffer.read(invert) & 0xff);
    }
    return v;
  }

  static int getCharacterMaxLength(TypeInfo type) {
    return ((BaseCharTypeInfo)type).getLength();
  }

  public static Text deserializeText(InputByteBuffer buffer, boolean invert, Text r)
      throws IOException {
    // Get the actual length first
    int start = buffer.tell();
    int length = 0;
    do {
      byte b = buffer.read(invert);
      if (b == 0) {
        // end of string
        break;
      }
      if (b == 1) {
        // the last char is an escape char. read the actual char
        buffer.read(invert);
      }
      length++;
    } while (true);

    if (length == buffer.tell() - start) {
      // No escaping happened, so we are already done.
      r.set(buffer.getData(), start, length);
    } else {
      // Escaping happened, we need to copy byte-by-byte.
      // 1. Set the length first.
      r.set(buffer.getData(), start, length);
      // 2. Reset the pointer.
      buffer.seek(start);
      // 3. Copy the data.
      byte[] rdata = r.getBytes();
      for (int i = 0; i < length; i++) {
        byte b = buffer.read(invert);
        if (b == 1) {
          // The last char is an escape char, read the actual char.
          // The serialization format escape \0 to \1, and \1 to \2,
          // to make sure the string is null-terminated.
          b = (byte) (buffer.read(invert) - 1);
        }
        rdata[i] = b;
      }
      // 4. Read the null terminator.
      byte b = buffer.read(invert);
      assert (b == 0);
    }
    return r;
  }

  BytesWritable serializeBytesWritable = new BytesWritable();
  ByteStream.Output output = new ByteStream.Output();

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    output.reset();
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < columnNames.size(); i++) {
      serialize(output, soi.getStructFieldData(obj, fields.get(i)),
          fields.get(i).getFieldObjectInspector(), columnSortOrderIsDesc[i],
          columnNullMarker[i], columnNotNullMarker[i]);
    }

    serializeBytesWritable.set(output.getData(), 0, output.getLength());
    return serializeBytesWritable;
  }

  public static void writeByte(RandomAccessOutput buffer, byte b, boolean invert) {
    if (invert) {
      b = (byte) (0xff ^ b);
    }
    buffer.write(b);
  }

  static void serialize(ByteStream.Output buffer, Object o, ObjectInspector oi,
      boolean invert, byte nullMarker, byte notNullMarker) throws SerDeException {
    // Is this field a null?
    if (o == null) {
      writeByte(buffer, nullMarker, invert);
      return;
    }
    // This field is not a null.
    writeByte(buffer, notNullMarker, invert);

    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      switch (poi.getPrimitiveCategory()) {
      case VOID: {
        return;
      }
      case BOOLEAN: {
        boolean v = ((BooleanObjectInspector) poi).get(o);
        writeByte(buffer, (byte) (v ? 2 : 1), invert);
        return;
      }
      case BYTE: {
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        byte v = boi.get(o);
        writeByte(buffer, (byte) (v ^ 0x80), invert);
        return;
      }
      case SHORT: {
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        short v = spoi.get(o);
        serializeShort(buffer, v, invert);
        return;
      }
      case INT: {
        IntObjectInspector ioi = (IntObjectInspector) poi;
        int v = ioi.get(o);
        serializeInt(buffer, v, invert);
        return;
      }
      case LONG: {
        LongObjectInspector loi = (LongObjectInspector) poi;
        long v = loi.get(o);
        serializeLong(buffer, v, invert);
        return;
      }
      case FLOAT: {
        FloatObjectInspector foi = (FloatObjectInspector) poi;
        serializeFloat(buffer, foi.get(o), invert);
        return;
      }
      case DOUBLE: {
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        serializeDouble(buffer, doi.get(o), invert);
        return;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector) poi;
        Text t = soi.getPrimitiveWritableObject(o);
        serializeBytes(buffer, t.getBytes(), t.getLength(), invert);
        return;
      }

      case CHAR: {
        HiveCharObjectInspector hcoi = (HiveCharObjectInspector) poi;
        HiveCharWritable hc = hcoi.getPrimitiveWritableObject(o);
        // Trailing space should ignored for char comparisons.
        // So write stripped values for this SerDe.
        Text t = hc.getStrippedValue();
        serializeBytes(buffer, t.getBytes(), t.getLength(), invert);
        return;
      }
      case VARCHAR: {
        HiveVarcharObjectInspector hcoi = (HiveVarcharObjectInspector)poi;
        HiveVarcharWritable hc = hcoi.getPrimitiveWritableObject(o);
        // use varchar's text field directly
        Text t = hc.getTextValue();
        serializeBytes(buffer, t.getBytes(), t.getLength(), invert);
        return;
      }

      case BINARY: {
        BinaryObjectInspector baoi = (BinaryObjectInspector) poi;
        BytesWritable ba = baoi.getPrimitiveWritableObject(o);
        byte[] toSer = new byte[ba.getLength()];
        System.arraycopy(ba.getBytes(), 0, toSer, 0, ba.getLength());
        serializeBytes(buffer, toSer, ba.getLength(), invert);
        return;
      }
      case  DATE: {
        DateObjectInspector doi = (DateObjectInspector) poi;
        int v = doi.getPrimitiveWritableObject(o).getDays();
        serializeInt(buffer, v, invert);
        return;
      }
      case TIMESTAMP: {
        TimestampObjectInspector toi = (TimestampObjectInspector) poi;
        TimestampWritableV2 t = toi.getPrimitiveWritableObject(o);
        serializeTimestampWritable(buffer, t, invert);
        return;
      }
      case TIMESTAMPLOCALTZ: {
        TimestampLocalTZObjectInspector toi = (TimestampLocalTZObjectInspector) poi;
        TimestampLocalTZWritable t = toi.getPrimitiveWritableObject(o);
        serializeTimestampTZWritable(buffer, t, invert);
        return;
      }
      case INTERVAL_YEAR_MONTH: {
        HiveIntervalYearMonthObjectInspector ioi = (HiveIntervalYearMonthObjectInspector) poi;
        HiveIntervalYearMonth intervalYearMonth = ioi.getPrimitiveJavaObject(o);
        serializeHiveIntervalYearMonth(buffer, intervalYearMonth, invert);
        return;
      }
      case INTERVAL_DAY_TIME: {
        HiveIntervalDayTimeObjectInspector ioi = (HiveIntervalDayTimeObjectInspector) poi;
        HiveIntervalDayTime intervalDayTime = ioi.getPrimitiveJavaObject(o);
        serializeHiveIntervalDayTime(buffer, intervalDayTime, invert);
        return;
      }
      case DECIMAL: {
        HiveDecimalObjectInspector boi = (HiveDecimalObjectInspector) poi;
        HiveDecimal dec = boi.getPrimitiveJavaObject(o);
        serializeHiveDecimal(buffer, dec, invert);
        return;
      }

      default: {
        throw new RuntimeException("Unrecognized type: "
            + poi.getPrimitiveCategory());
      }
      }
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      ObjectInspector eoi = loi.getListElementObjectInspector();

      // \1 followed by each element
      int size = loi.getListLength(o);
      for (int eid = 0; eid < size; eid++) {
        writeByte(buffer, (byte) 1, invert);
        serialize(buffer, loi.getListElement(o, eid), eoi, invert, nullMarker, notNullMarker);
      }
      // and \0 to terminate
      writeByte(buffer, (byte) 0, invert);
      return;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      // \1 followed by each key and then each value
      Map<?, ?> map = moi.getMap(o);
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        writeByte(buffer, (byte) 1, invert);
        serialize(buffer, entry.getKey(), koi, invert, nullMarker, notNullMarker);
        serialize(buffer, entry.getValue(), voi, invert, nullMarker, notNullMarker);
      }
      // and \0 to terminate
      writeByte(buffer, (byte) 0, invert);
      return;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();

      for (int i = 0; i < fields.size(); i++) {
        serialize(buffer, soi.getStructFieldData(o, fields.get(i)), fields.get(
            i).getFieldObjectInspector(), invert, nullMarker, notNullMarker);
      }
      return;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      byte tag = uoi.getTag(o);
      writeByte(buffer, tag, invert);
      serialize(buffer, uoi.getField(o), uoi.getObjectInspectors().get(tag),
          invert, nullMarker, notNullMarker);
      return;
    }
    default: {
      throw new RuntimeException("Unrecognized type: " + oi.getCategory());
    }
    }

  }

  public static void serializeBytes(
      ByteStream.Output buffer, byte[] data, int length, boolean invert) {
    for (int i = 0; i < length; i++) {
      if (data[i] == 0 || data[i] == 1) {
        writeByte(buffer, (byte) 1, invert);
        writeByte(buffer, (byte) (data[i] + 1), invert);
      } else {
        writeByte(buffer, data[i], invert);
      }
    }
    writeByte(buffer, (byte) 0, invert);
  }

  public static void serializeBytes(
      ByteStream.Output buffer, byte[] data, int offset, int length, boolean invert) {
    for (int i = offset; i < offset + length; i++) {
      if (data[i] == 0 || data[i] == 1) {
        writeByte(buffer, (byte) 1, invert);
        writeByte(buffer, (byte) (data[i] + 1), invert);
      } else {
        writeByte(buffer, data[i], invert);
      }
    }
    writeByte(buffer, (byte) 0, invert);
  }

  public static void serializeShort(ByteStream.Output buffer, short v, boolean invert) {
    writeByte(buffer, (byte) ((v >> 8) ^ 0x80), invert);
    writeByte(buffer, (byte) v, invert);
  }

  public static void serializeInt(ByteStream.Output buffer, int v, boolean invert) {
    writeByte(buffer, (byte) ((v >> 24) ^ 0x80), invert);
    writeByte(buffer, (byte) (v >> 16), invert);
    writeByte(buffer, (byte) (v >> 8), invert);
    writeByte(buffer, (byte) v, invert);
  }

  public static void serializeLong(ByteStream.Output buffer, long v, boolean invert) {
    writeByte(buffer, (byte) ((v >> 56) ^ 0x80), invert);
    writeByte(buffer, (byte) (v >> 48), invert);
    writeByte(buffer, (byte) (v >> 40), invert);
    writeByte(buffer, (byte) (v >> 32), invert);
    writeByte(buffer, (byte) (v >> 24), invert);
    writeByte(buffer, (byte) (v >> 16), invert);
    writeByte(buffer, (byte) (v >> 8), invert);
    writeByte(buffer, (byte) v, invert);
  }

  public static void serializeFloat(ByteStream.Output buffer, float vf, boolean invert) {
    int v = Float.floatToIntBits(vf);
    if ((v & (1 << 31)) != 0) {
      // negative number, flip all bits
      v = ~v;
    } else {
      // positive number, flip the first bit
      v = v ^ (1 << 31);
    }
    writeByte(buffer, (byte) (v >> 24), invert);
    writeByte(buffer, (byte) (v >> 16), invert);
    writeByte(buffer, (byte) (v >> 8), invert);
    writeByte(buffer, (byte) v, invert);
  }

  public static void serializeDouble(ByteStream.Output buffer, double vd, boolean invert) {
    long v = Double.doubleToLongBits(vd);
    if ((v & (1L << 63)) != 0) {
      // negative number, flip all bits
      v = ~v;
    } else {
      // positive number, flip the first bit
      v = v ^ (1L << 63);
    }
    writeByte(buffer, (byte) (v >> 56), invert);
    writeByte(buffer, (byte) (v >> 48), invert);
    writeByte(buffer, (byte) (v >> 40), invert);
    writeByte(buffer, (byte) (v >> 32), invert);
    writeByte(buffer, (byte) (v >> 24), invert);
    writeByte(buffer, (byte) (v >> 16), invert);
    writeByte(buffer, (byte) (v >> 8), invert);
    writeByte(buffer, (byte) v, invert);
  }

  public static void serializeTimestampWritable(ByteStream.Output buffer, TimestampWritableV2 t, boolean invert) {
    byte[] data = t.getBinarySortable();
    for (int i = 0; i < data.length; i++) {
      writeByte(buffer, data[i], invert);
    }
  }

  public static void serializeTimestampTZWritable(
      ByteStream.Output buffer, TimestampLocalTZWritable t, boolean invert) {
    byte[] data = t.toBinarySortable();
    for (byte b : data) {
      writeByte(buffer, b, invert);
    }
  }

  public static void serializeHiveIntervalYearMonth(ByteStream.Output buffer,
      HiveIntervalYearMonth intervalYearMonth, boolean invert) {
    int totalMonths = intervalYearMonth.getTotalMonths();
    serializeInt(buffer, totalMonths, invert);
  }

  public static void serializeHiveIntervalDayTime(ByteStream.Output buffer,
      HiveIntervalDayTime intervalDayTime, boolean invert) {
    long totalSecs = intervalDayTime.getTotalSeconds();
    int nanos = intervalDayTime.getNanos();
    serializeLong(buffer, totalSecs, invert);
    serializeInt(buffer, nanos, invert);
  }

  public static void serializeOldHiveDecimal(ByteStream.Output buffer, HiveDecimalV1 oldDec, boolean invert) {
    // get the sign of the big decimal
    int sign = oldDec.compareTo(HiveDecimalV1.ZERO);

    // we'll encode the absolute value (sign is separate)
    oldDec = oldDec.abs();

    // get the scale factor to turn big decimal into a decimal < 1
    // This relies on the BigDecimal precision value, which as of HIVE-10270
    // is now different from HiveDecimal.precision()
    int factor = oldDec.bigDecimalValue().precision() - oldDec.bigDecimalValue().scale();
    factor = sign == 1 ? factor : -factor;

    // convert the absolute big decimal to string
    oldDec.scaleByPowerOfTen(Math.abs(oldDec.scale()));
    String digits = oldDec.unscaledValue().toString();

    // finally write out the pieces (sign, scale, digits)
    writeByte(buffer, (byte) ( sign + 1), invert);
    writeByte(buffer, (byte) ((factor >> 24) ^ 0x80), invert);
    writeByte(buffer, (byte) ( factor >> 16), invert);
    writeByte(buffer, (byte) ( factor >> 8), invert);
    writeByte(buffer, (byte)   factor, invert);
    serializeBytes(buffer, digits.getBytes(decimalCharSet),
        digits.length(), sign == -1 ? !invert : invert);
  }

  // See comments for next method.
  public static void serializeHiveDecimal(ByteStream.Output buffer, HiveDecimal dec, boolean invert) {

    byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    serializeHiveDecimal(buffer, dec, invert, scratchBuffer);
  }

  /**
   * Decimals are encoded in three pieces:Decimals are encoded in three pieces:
   *
   * Sign:   1, 2 or 3 for smaller, equal or larger than 0 respectively
   * Factor: Number that indicates the amount of digits you have to move
   *         the decimal point left or right until the resulting number is smaller
   *         than zero but has something other than 0 as the first digit.
   * Digits: which is a string of all the digits in the decimal. If the number
   *         is negative the binary string will be inverted to get the correct ordering.
   *
   * UNDONE: Is this example correct?
   *   Example: 0.00123
   *   Sign is 3 (bigger than 0)
   *   Factor is -2 (move decimal point 2 positions right)
   *   Digits are: 123
   *
   * @param buffer
   * @param dec
   * @param invert
   * @param scratchBuffer
   */
  public static void serializeHiveDecimal(
    ByteStream.Output buffer, HiveDecimal dec, boolean invert,
    byte[] scratchBuffer) {

    // Get the sign of the decimal.
    int signum = dec.signum();

    // Get the 10^N power to turn digits into the desired decimal with a possible
    // fractional part.
    // To be compatible with the OldHiveDecimal version, zero has factor 1.
    int factor;
    if (signum == 0) {
      factor = 1;
    } else {
      factor = dec.rawPrecision() - dec.scale();
    }

    // To make comparisons work properly, the "factor" gets the decimal's sign, too.
    factor = signum == 1 ? factor : -factor;

    // Convert just the decimal digits (no dot, sign, etc) into bytes.
    //
    // This is much faster than converting the BigInteger value from unscaledValue() which is no
    // longer part of the HiveDecimal representation anymore to string, then bytes.
    int index = dec.toDigitsOnlyBytes(scratchBuffer);

    /*
     * Finally write out the pieces (sign, power, digits)
     */
    writeByte(buffer, (byte) ( signum + 1), invert);
    writeByte(buffer, (byte) ((factor >> 24) ^ 0x80), invert);
    writeByte(buffer, (byte) ( factor >> 16), invert);
    writeByte(buffer, (byte) ( factor >> 8), invert);
    writeByte(buffer, (byte)   factor, invert);

    // The toDigitsOnlyBytes stores digits at the end of the scratch buffer.
    serializeBytes(
        buffer,
        scratchBuffer, index, scratchBuffer.length - index,
        signum == -1 ? !invert : invert);
  }

  // A HiveDecimalWritable version.
  public static void serializeHiveDecimal(
      ByteStream.Output buffer, HiveDecimalWritable decWritable, boolean invert,
      byte[] scratchBuffer) {

      // Get the sign of the decimal.
      int signum = decWritable.signum();

      // Get the 10^N power to turn digits into the desired decimal with a possible
      // fractional part.
      // To be compatible with the OldHiveDecimal version, zero has factor 1.
      int factor;
      if (signum == 0) {
        factor = 1;
      } else {
        factor = decWritable.rawPrecision() - decWritable.scale();
      }

      // To make comparisons work properly, the "factor" gets the decimal's sign, too.
      factor = signum == 1 ? factor : -factor;

      // Convert just the decimal digits (no dot, sign, etc) into bytes.
      //
      // This is much faster than converting the BigInteger value from unscaledValue() which is no
      // longer part of the HiveDecimal representation anymore to string, then bytes.
      int index = decWritable.toDigitsOnlyBytes(scratchBuffer);

      /*
       * Finally write out the pieces (sign, power, digits)
       */
      writeByte(buffer, (byte) ( signum + 1), invert);
      writeByte(buffer, (byte) ((factor >> 24) ^ 0x80), invert);
      writeByte(buffer, (byte) ( factor >> 16), invert);
      writeByte(buffer, (byte) ( factor >> 8), invert);
      writeByte(buffer, (byte)   factor, invert);

      // The toDigitsOnlyBytes stores digits at the end of the scratch buffer.
      serializeBytes(
          buffer,
          scratchBuffer, index, scratchBuffer.length - index,
          signum == -1 ? !invert : invert);
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  public static void serializeStruct(Output byteStream, Object[] fieldData,
      List<ObjectInspector> fieldOis, boolean[] sortableSortOrders,
      byte[] nullMarkers, byte[] notNullMarkers) throws SerDeException {
    for (int i = 0; i < fieldData.length; i++) {
      serialize(byteStream, fieldData[i], fieldOis.get(i), sortableSortOrders[i],
              nullMarkers[i], notNullMarkers[i]);
    }
  }

  public boolean[] getSortOrders() {
    return columnSortOrderIsDesc;
  }

  public byte[] getNullMarkers() {
    return columnNullMarker;
  }

  public byte[] getNotNullMarkers() {
    return columnNotNullMarker;
  }

}
