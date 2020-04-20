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

package org.apache.hadoop.hive.serde2.binarysortable.fast;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.InputByteBuffer;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

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
public final class BinarySortableDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(BinarySortableDeserializeRead.class.getName());

  public static BinarySortableDeserializeRead with(TypeInfo[] typeInfos, boolean useExternalBuffer, Properties tbl) {
    boolean[] columnSortOrderIsDesc = new boolean[typeInfos.length];
    byte[] columnNullMarker = new byte[typeInfos.length];
    byte[] columnNotNullMarker = new byte[typeInfos.length];

    BinarySortableUtils.fillOrderArrays(tbl, columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker);

    return new BinarySortableDeserializeRead(
            typeInfos, useExternalBuffer, columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker);
  }

  /*
   * Use this factory method when only ascending sort order is used.
   */
  public static BinarySortableDeserializeRead ascendingNullsFirst(TypeInfo[] typeInfos, boolean useExternalBuffer) {
    final int count = typeInfos.length;

    boolean[] columnSortOrderIsDesc = new boolean[count];
    Arrays.fill(columnSortOrderIsDesc, false);

    byte[] columnNullMarker = new byte[count];
    byte[] columnNotNullMarker = new byte[count];
    for (int i = 0; i < count; i++) {
      // Ascending
      // Null first (default for ascending order)
      columnNullMarker[i] = BinarySortableSerDe.ZERO;
      columnNotNullMarker[i] = BinarySortableSerDe.ONE;
    }

    return new BinarySortableDeserializeRead(
            typeInfos, useExternalBuffer, columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker);
  }

  // The sort order (ascending/descending) for each field. Set to true when descending (invert).
  private boolean[] columnSortOrderIsDesc;

  byte[] columnNullMarker;
  byte[] columnNotNullMarker;

  private int start;
  private int end;
  private int fieldStart;

  private int bytesStart;

  private int internalBufferLen;
  private byte[] internalBuffer;

  private byte[] tempTimestampBytes;

  private byte[] tempDecimalBuffer;

  private InputByteBuffer inputByteBuffer = new InputByteBuffer();

  private Field root;
  private Deque<Field> stack;

  private class Field {
    Field[] children;

    Category category;
    PrimitiveObjectInspector.PrimitiveCategory primitiveCategory;
    TypeInfo typeInfo;

    int index;
    int count;
    int start;
    int tag;
  }

  public BinarySortableDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer,
      boolean[] columnSortOrderIsDesc, byte[] columnNullMarker, byte[] columnNotNullMarker) {
    this(typeInfos, null, useExternalBuffer, columnSortOrderIsDesc, columnNullMarker,
        columnNotNullMarker);
  }

  public BinarySortableDeserializeRead(TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      boolean useExternalBuffer, boolean[] columnSortOrderIsDesc, byte[] columnNullMarker, byte[] columnNotNullMarker) {
    super(typeInfos, dataTypePhysicalVariations, useExternalBuffer);
    final int count = typeInfos.length;

    root = new Field();
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = count;
    stack = new ArrayDeque<>();
    this.columnSortOrderIsDesc = columnSortOrderIsDesc;
    this.columnNullMarker = columnNullMarker;
    this.columnNotNullMarker = columnNotNullMarker;
    inputByteBuffer = new InputByteBuffer();
    internalBufferLen = -1;
  }

  // Not public since we must have column information.
  private BinarySortableDeserializeRead() {
    super();
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    start = offset;
    end = offset + length;
    inputByteBuffer.reset(bytes, start, end);
    root.index = -1;
    stack.clear();
    stack.push(root);
    clearIndex(root);
  }

  private void clearIndex(Field field) {
    field.index = -1;
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

    sb.append("Reading inputByteBuffer of length ");
    sb.append(inputByteBuffer.getEnd());
    sb.append(" at start offset ");
    sb.append(start);
    sb.append(" for length ");
    sb.append(end - start);
    sb.append(" to read ");
    sb.append(root.count);
    sb.append(" fields with types ");
    sb.append(Arrays.toString(typeInfos));
    sb.append(".  ");
    if (root.index == -1) {
      sb.append("Before first field?");
    } else {
      sb.append("Read field #");
      sb.append(root.index);
      sb.append(" at field start position ");
      sb.append(fieldStart);
      sb.append(" current read offset ");
      sb.append(inputByteBuffer.tell());
    }
    sb.append(" column sort order ");
    sb.append(Arrays.toString(columnSortOrderIsDesc));
    // UNDONE: Convert byte 0 or 1 to character.
    sb.append(" column null marker ");
    sb.append(Arrays.toString(columnNullMarker));
    sb.append(" column non null marker ");
    sb.append(Arrays.toString(columnNotNullMarker));

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
    final int fieldIndex = root.index;
    field.start = inputByteBuffer.tell();

    /*
     * We have a field and are positioned to it.  Read it.
     */
    switch (field.primitiveCategory) {
    case BOOLEAN:
      currentBoolean = (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) == 2);
      return true;
    case BYTE:
      currentByte = (byte) (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) ^ 0x80);
      return true;
    case SHORT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        currentShort = (short) v;
      }
      return true;
    case INT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentInt = v;
      }
      return true;
    case LONG:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentLong = v;
      }
      return true;
    case DATE:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentDateWritable.set(v);
      }
      return true;
    case TIMESTAMP:
      {
        if (tempTimestampBytes == null) {
          tempTimestampBytes = new byte[TimestampWritableV2.BINARY_SORTABLE_LENGTH];
        }
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        for (int i = 0; i < tempTimestampBytes.length; i++) {
          tempTimestampBytes[i] = inputByteBuffer.read(invert);
        }
        currentTimestampWritable.setBinarySortable(tempTimestampBytes, 0);
      }
      return true;
    case FLOAT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = 0;
        for (int i = 0; i < 4; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        if ((v & (1 << 31)) == 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1 << 31);
        }
        currentFloat = Float.intBitsToFloat(v);
      }
      return true;
    case DOUBLE:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long v = 0;
        for (int i = 0; i < 8; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        if ((v & (1L << 63)) == 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1L << 63);
        }
        currentDouble = Double.longBitsToDouble(v);
      }
      return true;
    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        /*
         * This code is a modified version of BinarySortableSerDe.deserializeText that lets us
         * detect if we can return a reference to the bytes directly.
         */

        // Get the actual length first
        bytesStart = inputByteBuffer.tell();
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int length = 0;
        do {
          byte b = inputByteBuffer.read(invert);
          if (b == 0) {
            // end of string
            break;
          }
          if (b == 1) {
            // the last char is an escape char. read the actual char
            inputByteBuffer.read(invert);
          }
          length++;
        } while (true);

        if (length == 0 ||
            (!invert && length == inputByteBuffer.tell() - bytesStart - 1)) {
          // No inversion or escaping happened, so we are can reference directly.
          currentExternalBufferNeeded = false;
          currentBytes = inputByteBuffer.getData();
          currentBytesStart = bytesStart;
          currentBytesLength = length;
        } else {
          // We are now positioned at the end of this field's bytes.
          if (useExternalBuffer) {
            // If we decided not to reposition and re-read the buffer to copy it with
            // copyToExternalBuffer, we we will still be correctly positioned for the next field.
            currentExternalBufferNeeded = true;
            currentExternalBufferNeededLen = length;
          } else {
            // The copyToBuffer will reposition and re-read the input buffer.
            currentExternalBufferNeeded = false;
            if (internalBufferLen < length) {
              internalBufferLen = length;
              internalBuffer = new byte[internalBufferLen];
            }
            copyToBuffer(internalBuffer, 0, length);
            currentBytes = internalBuffer;
            currentBytesStart = 0;
            currentBytesLength = length;
          }
        }
      }
      return true;
    case INTERVAL_YEAR_MONTH:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentHiveIntervalYearMonthWritable.set(v);
      }
      return true;
    case INTERVAL_DAY_TIME:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long totalSecs = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          totalSecs = (totalSecs << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        int nanos = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          nanos = (nanos << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentHiveIntervalDayTimeWritable.set(totalSecs, nanos);
      }
      return true;
    case DECIMAL:
      {
        // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
        // we must read it, enforce it here, and either return NULL or buffer the result.

        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int b = inputByteBuffer.read(invert) - 1;
        if (!(b == 1 || b == -1 || b == 0)) {
          throw new IOException("Unexpected byte value " + (int)b + " in binary sortable format data (invert " + invert + ")");
        }
        final boolean positive = b != -1;

        int factor = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          factor = (factor << 8) + (inputByteBuffer.read(invert) & 0xff);
        }

        if (!positive) {
          factor = -factor;
        }

        final int decimalStart = inputByteBuffer.tell();
        int length = 0;

        do {
          b = inputByteBuffer.read(positive ? invert : !invert);
          if (b == 1) {
            throw new IOException("Expected -1 and found byte value " + (int)b + " in binary sortable format data (invert " + invert + ")");
          }


          if (b == 0) {
            // end of digits
            break;
          }

          length++;
        } while (true);

        // CONSIDER: Allocate a larger initial size.
        if(tempDecimalBuffer == null || tempDecimalBuffer.length < length) {
          tempDecimalBuffer = new byte[length];
        }

        inputByteBuffer.seek(decimalStart);
        for (int i = 0; i < length; ++i) {
          tempDecimalBuffer[i] = inputByteBuffer.read(positive ? invert : !invert);
        }

        // read the null byte again
        inputByteBuffer.read(positive ? invert : !invert);

        // Set the value of the writable from the decimal digits that were written with no dot.
        final int scale = length - factor;
        currentHiveDecimalWritable.setFromDigitsOnlyBytesWithScale(
            !positive, tempDecimalBuffer, 0, length, scale);
        boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
        if (!decimalIsNull) {

          // We have a decimal.  After we enforce precision and scale, will it become a NULL?

          final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) field.typeInfo;

          final int enforcePrecision = decimalTypeInfo.getPrecision();
          final int enforceScale = decimalTypeInfo.getScale();

          decimalIsNull =
              !currentHiveDecimalWritable.mutateEnforcePrecisionScale(
                  enforcePrecision, enforceScale);

        }
        if (decimalIsNull) {
          return false;
        }
      }
      return true;
    default:
      throw new RuntimeException("Unexpected primitive type category " + field.primitiveCategory);
    }
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    final Field current = stack.peek();
    current.index++;

    if (root.index >= root.count) {
      return;
    }

    if (inputByteBuffer.isEof()) {
      // Also, reading beyond our byte range produces NULL.
      return;
    }

    if (current.category == Category.UNION && current.index == 0) {
      current.tag = inputByteBuffer.read();
      currentInt = current.tag;
      return;
    }

    final Field child = getChild(current);

    if (isNull()) {
      return;
    }
    if (child.category == Category.PRIMITIVE) {
      readPrimitive(child);
    } else {
      stack.push(child);
      switch (child.category) {
      case LIST:
      case MAP:
        while (isNextComplexMultiValue()) {
          skipNextField();
        }
        break;
      case STRUCT:
        for (int i = 0; i < child.count; i++) {
          skipNextField();
        }
        finishComplexVariableFieldsType();
        break;
      case UNION:
        readComplexField();
        skipNextField();
        finishComplexVariableFieldsType();
        break;
      }
    }
  }

  @Override
  public void copyToExternalBuffer(byte[] externalBuffer, int externalBufferStart) throws IOException {
    copyToBuffer(externalBuffer, externalBufferStart, currentExternalBufferNeededLen);
  }

  private void copyToBuffer(byte[] buffer, int bufferStart, int bufferLength) throws IOException {
    final boolean invert = columnSortOrderIsDesc[root.index];
    inputByteBuffer.seek(bytesStart);
    // 3. Copy the data.
    for (int i = 0; i < bufferLength; i++) {
      byte b = inputByteBuffer.read(invert);
      if (b == 1) {
        // The last char is an escape char, read the actual char.
        // The serialization format escape \0 to \1, and \1 to \2,
        // to make sure the string is null-terminated.
        b = (byte) (inputByteBuffer.read(invert) - 1);
      }
      buffer[bufferStart + i] = b;
    }
    // 4. Read the null terminator.
    byte b = inputByteBuffer.read(invert);
    if (b != 0) {
      throw new RuntimeException("Expected 0 terminating byte");
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
    return inputByteBuffer.isEof();
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
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
      field.count = fieldTypeInfos.size();
      field.children = createFields(fieldTypeInfos.toArray(new TypeInfo[fieldTypeInfos.size()]));
      break;
    case UNION:
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
      field.count = 2;
      field.children = createFields(objectTypeInfos.toArray(new TypeInfo[objectTypeInfos.size()]));
      break;
    default:
      throw new RuntimeException();
    }
    return field;
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

  private boolean isNull() throws IOException {
    return inputByteBuffer.read(columnSortOrderIsDesc[root.index]) ==
        columnNullMarker[root.index];
  }

  @Override
  public boolean readComplexField() throws IOException {
    final Field current = stack.peek();
    current.index++;

    if (root.index >= root.count) {
      return false;
    }

    if (inputByteBuffer.isEof()) {
      // Also, reading beyond our byte range produces NULL.
      return false;
    }

    if (current.category == Category.UNION) {
      if (current.index == 0) {
        current.tag = inputByteBuffer.read(columnSortOrderIsDesc[root.index]);
        currentInt = current.tag;
        return true;
      }
    }

    final Field child = getChild(current);

    boolean isNull = isNull();

    if (isNull) {
      return false;
    }
    if (child.category == Category.PRIMITIVE) {
      isNull = !readPrimitive(child);
    } else {
      stack.push(child);
    }
    return !isNull;
  }

  @Override
  public boolean isNextComplexMultiValue() throws IOException {
    final byte isNullByte = inputByteBuffer.read(columnSortOrderIsDesc[root.index]);
    final boolean isEnded;

    switch (isNullByte) {
    case 0:
      isEnded = true;
      break;

    case 1:
      isEnded = false;
      break;

    default:
      throw new RuntimeException();
    }

    if (isEnded) {
      stack.pop();
      stack.peek();
    }
    return !isEnded;
  }

  @Override
  public void finishComplexVariableFieldsType() {
    stack.pop();
    if (stack.peek() == null) {
      throw new RuntimeException();
    }
    stack.peek();
  }
}
