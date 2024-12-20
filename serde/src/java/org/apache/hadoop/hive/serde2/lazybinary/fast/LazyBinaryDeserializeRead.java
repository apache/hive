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

package org.apache.hadoop.hive.serde2.lazybinary.fast;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Directly deserialize with the caller reading field-by-field the LazyBinary serialization format.
 *
 * The caller is responsible for calling the read method for the right type of each field
 * (after calling readNextField).
 *
 * Reading some fields require a results object to receive value information.  A separate
 * results object is created by the caller at initialization per different field even for the same
 * type.
 *
 * Some type values are by reference to either bytes in the deserialization buffer or to
 * other type specific buffers.  So, those references are only valid until the next time set is
 * called.
 */
public final class LazyBinaryDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(LazyBinaryDeserializeRead.class.getName());

  private byte[] bytes;
  private int start;
  private int offset;
  private int end;

  private boolean skipLengthPrefix = false;

  // Object to receive results of reading a decoded variable length int or long.
  private VInt tempVInt;
  private VLong tempVLong;

  private Deque<Field> stack = new ArrayDeque<>();
  private Field root;

  private class Field {
    Field[] children;

    Category category;
    PrimitiveCategory primitiveCategory;
    TypeInfo typeInfo;
    DataTypePhysicalVariation dataTypePhysicalVariation;

    int index;
    int count;
    int start;
    int end;
    int nullByteStart;
    byte nullByte;
    byte tag;
  }

  public LazyBinaryDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer) {
    this(typeInfos, null, useExternalBuffer);
  }

  public LazyBinaryDeserializeRead(TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      boolean useExternalBuffer) {
    super(typeInfos, dataTypePhysicalVariations, useExternalBuffer);
    tempVInt = new VInt();
    tempVLong = new VLong();
    currentExternalBufferNeeded = false;

    root = new Field();
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = typeInfos.length;
  }

  private Field[] createFields(TypeInfo[] typeInfos) {
    final Field[] children = new Field[typeInfos.length];
    for (int i = 0; i < typeInfos.length; i++) {
      children[i] = createField(typeInfos[i]);
    }
    return children;
  }

  private Field createField(TypeInfo typeInfo) {
    final Field field = new Field();
    final Category category = typeInfo.getCategory();
    field.category = category;
    field.typeInfo = typeInfo;
    switch (category) {
    case PRIMITIVE:
      field.primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      break;
    case LIST:
      field.children = new Field[1];
      field.children[0] = createField(((ListTypeInfo) typeInfo).getListElementTypeInfo());
      break;
    case MAP:
      field.children = new Field[2];
      field.children[0] = createField(((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
      field.children[1] = createField(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
      break;
    case STRUCT:
      final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
      field.children = createFields(fieldTypeInfos.toArray(new TypeInfo[fieldTypeInfos.size()]));
      break;
    case UNION:
      final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
      field.children = createFields(objectTypeInfos.toArray(new TypeInfo[objectTypeInfos.size()]));
      break;
    default:
      throw new RuntimeException();
    }
    return field;
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    start = offset;
    end = offset + length;

    stack.clear();
    stack.push(root);
    clearIndex(root);
  }

  private void clearIndex(Field field) {
    field.index = 0;
    if (field.children == null) {
      return;
    }
    for (Field child : field.children) {
      clearIndex(child);
    }
  }

  /*
   * Get detailed read position information to help diagnose exceptions.
   */
  public String getDetailedReadPositionString() {
    StringBuilder sb = new StringBuilder(64);

    sb.append("Reading byte[] of length ");
    sb.append(bytes.length);
    sb.append(" at start offset ");
    sb.append(start);
    sb.append(" for length ");
    sb.append(end - start);
    sb.append(" to read ");
    sb.append(root.children.length);
    sb.append(" fields with types ");
    sb.append(Arrays.toString(typeInfos));
    sb.append(".  Read field #");
    sb.append(root.index);
    sb.append(" at field start position ");
    sb.append(root.start);
    sb.append(" current read offset ");
    sb.append(offset);

    return sb.toString();
  }

  /*
   * Reads the the next field.
   *
   * Afterwards, reading is positioned to the next field.
   *
   * @return  Return true when the field was not null and data is put in the appropriate
   *          current* member.
   *          Otherwise, false when the field is null.
   *
   */
  @Override
  public boolean readNextField() throws IOException {
    return readComplexField();
  }

  private boolean readPrimitive(Field field) throws IOException {
    final PrimitiveCategory primitiveCategory = field.primitiveCategory;
    final TypeInfo typeInfo = field.typeInfo;
    switch (primitiveCategory) {
    case BOOLEAN:
      // No check needed for single byte read.
      currentBoolean = (bytes[offset++] != 0);
      break;
    case BYTE:
      // No check needed for single byte read.
      currentByte = bytes[offset++];
      break;
    case SHORT:
      // Last item -- ok to be at end.
      if (offset + 2 > end) {
        throw new EOFException();
      }
      currentShort = LazyBinaryUtils.byteArrayToShort(bytes, offset);
      offset += 2;
      break;
    case INT:
      // Parse the first byte of a vint/vlong to determine the number of bytes.
      if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
        throw new EOFException();
      }
      LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
      offset += tempVInt.length;
      currentInt = tempVInt.value;
      break;
    case LONG:
      // Parse the first byte of a vint/vlong to determine the number of bytes.
      if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
        throw new EOFException();
      }
      LazyBinaryUtils.readVLong(bytes, offset, tempVLong);
      offset += tempVLong.length;
      currentLong = tempVLong.value;
      break;
    case FLOAT:
      // Last item -- ok to be at end.
      if (offset + 4 > end) {
        throw new EOFException();
      }
      currentFloat = Float.intBitsToFloat(LazyBinaryUtils.byteArrayToInt(bytes, offset));
      offset += 4;
      break;
    case DOUBLE:
      // Last item -- ok to be at end.
      if (offset + 8 > end) {
        throw new EOFException();
      }
      currentDouble = Double.longBitsToDouble(LazyBinaryUtils.byteArrayToLong(bytes, offset));
      offset += 8;
      break;

    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        // using vint instead of 4 bytes
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;

        int saveStart = offset;
        int length = tempVInt.value;
        offset += length;
        // Last item -- ok to be at end.
        if (offset > end) {
          throw new EOFException();
        }

        currentBytes = bytes;
        currentBytesStart = saveStart;
        currentBytesLength = length;
      }
      break;
    case DATE:
      // Parse the first byte of a vint/vlong to determine the number of bytes.
      if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
        throw new EOFException();
      }
      LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
      offset += tempVInt.length;

      currentDateWritable.set(tempVInt.value);
      break;
    case TIMESTAMP:
      {
        int length = TimestampWritableV2.getTotalLength(bytes, offset);
        int saveStart = offset;
        offset += length;
        // Last item -- ok to be at end.
        if (offset > end) {
          throw new EOFException();
        }

        currentTimestampWritable.set(bytes, saveStart);
      }
      break;
    case INTERVAL_YEAR_MONTH:
      // Parse the first byte of a vint/vlong to determine the number of bytes.
      if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
        throw new EOFException();
      }
      LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
      offset += tempVInt.length;

      currentHiveIntervalYearMonthWritable.set(tempVInt.value);
      break;
    case INTERVAL_DAY_TIME:
      // The first bounds check requires at least one more byte beyond for 2nd int (hence >=).
      // Parse the first byte of a vint/vlong to determine the number of bytes.
      if (offset + WritableUtils.decodeVIntSize(bytes[offset]) >= end) {
        throw new EOFException();
      }
      LazyBinaryUtils.readVLong(bytes, offset, tempVLong);
      offset += tempVLong.length;

      // Parse the first byte of a vint/vlong to determine the number of bytes.
      if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
        throw new EOFException();
      }
      LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
      offset += tempVInt.length;

      currentHiveIntervalDayTimeWritable.set(tempVLong.value, tempVInt.value);
      break;
    case DECIMAL:
      {
        // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
        // we must read it, enforce it here, and either return NULL or buffer the result.

        // These calls are to see how much data there is. The setFromBytes call below will do the same
        // readVInt reads but actually unpack the decimal.

        // The first bounds check requires at least one more byte beyond for 2nd int (hence >=).
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) >= end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;
        int readScale = tempVInt.value;

        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;
        int saveStart = offset;
        offset += tempVInt.value;
        // Last item -- ok to be at end.
        if (offset > end) {
          throw new EOFException();
        }
        int length = offset - saveStart;

        //   scale = 2, length = 6, value = -6065716379.11
        //   \002\006\255\114\197\131\083\105
        //           \255\114\197\131\083\105

        currentHiveDecimalWritable.setFromBigIntegerBytesAndScale(
            bytes, saveStart, length, readScale);
        boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
        if (!decimalIsNull) {

          final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;

          final int precision = decimalTypeInfo.getPrecision();
          final int scale = decimalTypeInfo.getScale();

          decimalIsNull = !currentHiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
          if (!decimalIsNull) {
            return true;
          }
        }
        return false;
      }
    default:
      throw new Error("Unexpected primitive category " + primitiveCategory.name());
    }
    return true;
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    final Field current = stack.peek();
    final boolean isNull = isNull(current);

    if (isNull) {
      current.index++;
      return;
    }

    if (readUnionTag(current)) {
      current.index++;
      return;
    }

    final Field child = getChild(current);

    if (child.category == Category.PRIMITIVE) {
      readPrimitive(child);
      current.index++;
    } else {
      parseHeader(child);
      stack.push(child);

      for (int i = 0; i < child.count; i++) {
        skipNextField();
      }
      finishComplexVariableFieldsType();
    }

    if (offset > end) {
      throw new EOFException();
    }
  }

  /*
   * Call this method may be called after all the all fields have been read to check
   * for unread fields.
   *
   * Note that when optimizing reading to stop reading unneeded include columns, worrying
   * about whether all data is consumed is not appropriate (often we aren't reading it all by
   * design).
   *
   * Since LazySimpleDeserializeRead parses the line through the last desired column it does
   * support this function.
   */
  public boolean isEndOfInputReached() {
    return (offset == end);
  }

  private boolean isNull(Field field) {
    final byte b = (byte) (1 << (field.index % 8));
    switch (field.category) {
    case PRIMITIVE:
      return false;
    case LIST:
    case MAP:
      final byte nullByte = bytes[field.nullByteStart + (field.index / 8)];
      return (nullByte & b) == 0;
    case STRUCT:
      if (field.index % 8 == 0) {
        field.nullByte = bytes[offset++];
      }
      return (field.nullByte & b) == 0;
    case UNION:
      return false;
    default:
      throw new RuntimeException();
    }
  }

  private void parseHeader(Field field) {
    // Init
    field.index = 0;
    field.start = offset;

    // Read length
    if (!skipLengthPrefix) {
      final int length = LazyBinaryUtils.byteArrayToInt(bytes, offset);
      offset += 4;
      field.end = offset + length;
    }

    switch (field.category) {
    case LIST:
    case MAP:
      // Read count
      LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
      if (field.category == Category.LIST) {
        field.count = tempVInt.value;
      } else {
        field.count = tempVInt.value * 2;
      }
      offset += tempVInt.length;

      // Null byte start
      field.nullByteStart = offset;
      offset += ((field.count) + 7) / 8;
      break;
    case STRUCT:
      field.count = ((StructTypeInfo) field.typeInfo).getAllStructFieldTypeInfos().size();
      break;
    case UNION:
      field.count = 2;
      break;
    }
  }

  private Field getChild(Field field) {
    switch (field.category) {
    case LIST:
      return field.children[0];
    case MAP:
      return field.children[field.index % 2];
    case STRUCT:
      return field.children[field.index];
    case UNION:
      return field.children[field.tag];
    default:
      throw new RuntimeException();
    }
  }

  private boolean readUnionTag(Field field) {
    if (field.category == Category.UNION && field.index == 0) {
      field.tag = bytes[offset++];
      currentInt = field.tag;
      return true;
    } else {
      return false;
    }
  }

  // Push or next
  @Override
  public boolean readComplexField() throws IOException {
    final Field current = stack.peek();
    boolean isNull = isNull(current);

    if (isNull) {
      current.index++;
      return false;
    }

    if (readUnionTag(current)) {
      current.index++;
      return true;
    }

    final Field child = getChild(current);

    if (child.category == Category.PRIMITIVE) {
      isNull = !readPrimitive(child);
      current.index++;
    } else {
      parseHeader(child);
      stack.push(child);
    }

    if (offset > end) {
      throw new EOFException();
    }
    return !isNull;
  }

  // Pop (list, map)
  @Override
  public boolean isNextComplexMultiValue() {
    Field current = stack.peek();
    final boolean isNext = current.index < current.count;
    if (!isNext) {
      stack.pop();
      stack.peek().index++;
    }
    return isNext;
  }

  // Pop (struct, union)
  @Override
  public void finishComplexVariableFieldsType() {
    stack.pop();
    if (stack.peek() == null) {
      throw new RuntimeException();
    }
    stack.peek().index++;
  }
}
