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

package org.apache.hadoop.hive.serde2.binarysortable;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
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

/**
 * BinarySortableSerDe can be used to write data in a way that the data can be
 * compared byte-by-byte with the same order.
 *
 * The data format: NULL: a single byte \0 NON-NULL Primitives: ALWAYS prepend a
 * single byte \1, and then: Boolean: FALSE = \1, TRUE = \2 Byte: flip the
 * sign-bit to make sure negative comes before positive Short: flip the sign-bit
 * to make sure negative comes before positive Int: flip the sign-bit to make
 * sure negative comes before positive Long: flip the sign-bit to make sure
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
 */
public class BinarySortableSerDe extends AbstractSerDe {

  public static final Log LOG = LogFactory.getLog(BinarySortableSerDe.class
      .getName());

  List<String> columnNames;
  List<TypeInfo> columnTypes;

  TypeInfo rowTypeInfo;
  StructObjectInspector rowObjectInspector;

  boolean[] columnSortOrderIsDesc;

  private static byte[] decimalBuffer = null;
  private static Charset decimalCharSet = Charset.forName("US-ASCII");

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // Get column names and sort order
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
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
            columnSortOrderIsDesc[i], row.get(i)));
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return row;
  }

  static Object deserialize(InputByteBuffer buffer, TypeInfo type,
      boolean invert, Object reuse) throws IOException {

    // Is this field a null?
    byte isNull = buffer.read(invert);
    if (isNull == 0) {
      return null;
    }
    assert (isNull == 1);

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
        int v = buffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        r.set(v);
        return r;
      }
      case LONG: {
        LongWritable r = reuse == null ? new LongWritable()
            : (LongWritable) reuse;
        long v = buffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        r.set(v);
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

      case TIMESTAMP:
        TimestampWritable t = (reuse == null ? new TimestampWritable() :
            (TimestampWritable) reuse);
        byte[] bytes = new byte[8];

        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = buffer.read(invert);
        }
        t.setBinarySortable(bytes, 0);
        return t;

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

        if(decimalBuffer == null || decimalBuffer.length < length) {
          decimalBuffer = new byte[length];
        }

        buffer.seek(start);
        for (int i = 0; i < length; ++i) {
          decimalBuffer[i] = buffer.read(positive ? invert : !invert);
        }

        // read the null byte again
        buffer.read(positive ? invert : !invert);

        String digits = new String(decimalBuffer, 0, length, decimalCharSet);
        BigInteger bi = new BigInteger(digits);
        HiveDecimal bd = new HiveDecimal(bi).scaleByPowerOfTen(factor-length);

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
        r.set(size, deserialize(buffer, etype, invert, r.get(size)));
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
      if (reuse == null) {
        r = new HashMap<Object, Object>();
      } else {
        r = (HashMap<Object, Object>) reuse;
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
        Object k = deserialize(buffer, ktype, invert, null);
        Object v = deserialize(buffer, vtype, invert, null);
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
            .set(eid, deserialize(buffer, fieldTypes.get(eid), invert, r
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
          invert, null));
      return r;
    }
    default: {
      throw new RuntimeException("Unrecognized type: " + type.getCategory());
    }
    }
  }

  BytesWritable serializeBytesWritable = new BytesWritable();
  OutputByteBuffer outputByteBuffer = new OutputByteBuffer();

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    outputByteBuffer.reset();
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < columnNames.size(); i++) {
      serialize(outputByteBuffer, soi.getStructFieldData(obj, fields.get(i)),
          fields.get(i).getFieldObjectInspector(), columnSortOrderIsDesc[i]);
    }

    serializeBytesWritable.set(outputByteBuffer.getData(), 0, outputByteBuffer
        .getLength());
    return serializeBytesWritable;
  }

  static void serialize(OutputByteBuffer buffer, Object o, ObjectInspector oi,
      boolean invert) {
    // Is this field a null?
    if (o == null) {
      buffer.write((byte) 0, invert);
      return;
    }
    // This field is not a null.
    buffer.write((byte) 1, invert);

    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      switch (poi.getPrimitiveCategory()) {
      case VOID: {
        return;
      }
      case BOOLEAN: {
        boolean v = ((BooleanObjectInspector) poi).get(o);
        buffer.write((byte) (v ? 2 : 1), invert);
        return;
      }
      case BYTE: {
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        byte v = boi.get(o);
        buffer.write((byte) (v ^ 0x80), invert);
        return;
      }
      case SHORT: {
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        short v = spoi.get(o);
        buffer.write((byte) ((v >> 8) ^ 0x80), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case INT: {
        IntObjectInspector ioi = (IntObjectInspector) poi;
        int v = ioi.get(o);
        buffer.write((byte) ((v >> 24) ^ 0x80), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case LONG: {
        LongObjectInspector loi = (LongObjectInspector) poi;
        long v = loi.get(o);
        buffer.write((byte) ((v >> 56) ^ 0x80), invert);
        buffer.write((byte) (v >> 48), invert);
        buffer.write((byte) (v >> 40), invert);
        buffer.write((byte) (v >> 32), invert);
        buffer.write((byte) (v >> 24), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case FLOAT: {
        FloatObjectInspector foi = (FloatObjectInspector) poi;
        int v = Float.floatToIntBits(foi.get(o));
        if ((v & (1 << 31)) != 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1 << 31);
        }
        buffer.write((byte) (v >> 24), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case DOUBLE: {
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        long v = Double.doubleToLongBits(doi.get(o));
        if ((v & (1L << 63)) != 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1L << 63);
        }
        buffer.write((byte) (v >> 56), invert);
        buffer.write((byte) (v >> 48), invert);
        buffer.write((byte) (v >> 40), invert);
        buffer.write((byte) (v >> 32), invert);
        buffer.write((byte) (v >> 24), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector) poi;
        Text t = soi.getPrimitiveWritableObject(o);
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
      case TIMESTAMP: {
        TimestampObjectInspector toi = (TimestampObjectInspector) poi;
        TimestampWritable t = toi.getPrimitiveWritableObject(o);
        byte[] data = t.getBinarySortable();
        for (int i = 0; i < data.length; i++) {
          buffer.write(data[i], invert);
        }
        return;
      }
      case DECIMAL: {
        // decimals are encoded in three pieces:
        // sign: 1, 2 or 3 for smaller, equal or larger than 0 respectively
        // factor: Number that indicates the amount of digits you have to move
        // the decimal point left or right until the resulting number is smaller
        // than zero but has something other than 0 as the first digit.
        // digits: which is a string of all the digits in the decimal. If the number
        // is negative the binary string will be inverted to get the correct ordering.
        // Example: 0.00123
        // Sign is 3 (bigger than 0)
        // Factor is -2 (move decimal point 2 positions right)
        // Digits are: 123

        HiveDecimalObjectInspector boi = (HiveDecimalObjectInspector) poi;
        HiveDecimal dec = boi.getPrimitiveJavaObject(o);

        // get the sign of the big decimal
        int sign = dec.compareTo(HiveDecimal.ZERO);

        // we'll encode the absolute value (sign is separate)
        dec = dec.abs();

        // get the scale factor to turn big decimal into a decimal < 1
        int factor = dec.precision() - dec.scale();
        factor = sign == 1 ? factor : -factor;

        // convert the absolute big decimal to string
        dec.scaleByPowerOfTen(Math.abs(dec.scale()));
        String digits = dec.unscaledValue().toString();

        // finally write out the pieces (sign, scale, digits)
        buffer.write((byte) ( sign + 1), invert);
        buffer.write((byte) ((factor >> 24) ^ 0x80), invert);
        buffer.write((byte) ( factor >> 16), invert);
        buffer.write((byte) ( factor >> 8), invert);
        buffer.write((byte)   factor, invert);
        serializeBytes(buffer, digits.getBytes(decimalCharSet),
            digits.length(), sign == -1 ? !invert : invert);
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
        buffer.write((byte) 1, invert);
        serialize(buffer, loi.getListElement(o, eid), eoi, invert);
      }
      // and \0 to terminate
      buffer.write((byte) 0, invert);
      return;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      // \1 followed by each key and then each value
      Map<?, ?> map = moi.getMap(o);
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        buffer.write((byte) 1, invert);
        serialize(buffer, entry.getKey(), koi, invert);
        serialize(buffer, entry.getValue(), voi, invert);
      }
      // and \0 to terminate
      buffer.write((byte) 0, invert);
      return;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();

      for (int i = 0; i < fields.size(); i++) {
        serialize(buffer, soi.getStructFieldData(o, fields.get(i)), fields.get(
            i).getFieldObjectInspector(), invert);
      }
      return;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      byte tag = uoi.getTag(o);
      buffer.write(tag, invert);
      serialize(buffer, uoi.getField(o), uoi.getObjectInspectors().get(tag),
          invert);
      return;
    }
    default: {
      throw new RuntimeException("Unrecognized type: " + oi.getCategory());
    }
    }

  }

  private static void serializeBytes(OutputByteBuffer buffer, byte[] data, int length, boolean invert){
    for (int i = 0; i < length; i++) {
      if (data[i] == 0 || data[i] == 1) {
        buffer.write((byte) 1, invert);
        buffer.write((byte) (data[i] + 1), invert);
      } else {
        buffer.write(data[i], invert);
      }
    }
    buffer.write((byte) 0, invert);
  }
  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
