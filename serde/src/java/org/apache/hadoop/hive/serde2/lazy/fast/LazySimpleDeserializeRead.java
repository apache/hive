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

package org.apache.hadoop.hive.serde2.lazy.fast;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.type.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.TimestampParser;

import com.google.common.base.Preconditions;

/*
 * Directly deserialize with the caller reading field-by-field the LazySimple (text)
 * serialization format.
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
public final class LazySimpleDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(LazySimpleDeserializeRead.class.getName());

  /*
   * Information on a field.  Made a class to allow readField to be agnostic to whether a top level
   * or field within a complex type is being read
   */
  private static class Field {

    // Optimize for most common case -- primitive.
    public final boolean isPrimitive;
    public final PrimitiveCategory primitiveCategory;

    public final Category complexCategory;

    public final TypeInfo typeInfo;
    public final DataTypePhysicalVariation dataTypePhysicalVariation;

    public ComplexTypeHelper complexTypeHelper;

    public Field(TypeInfo typeInfo, DataTypePhysicalVariation dataTypePhysicalVariation) {
      Category category = typeInfo.getCategory();
      if (category == Category.PRIMITIVE) {
        isPrimitive = true;
        primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        complexCategory = null;
      } else {
        isPrimitive = false;
        primitiveCategory = null;
        complexCategory = category;
      }

      this.typeInfo = typeInfo;
      this.dataTypePhysicalVariation = dataTypePhysicalVariation;

      complexTypeHelper = null;
    }

    public Field(TypeInfo typeInfo) {
      this(typeInfo, DataTypePhysicalVariation.NONE);
    }
  }

  /*
   * Used to keep position/length for complex type fields.
   * NOTE: The top level uses startPositions instead.
   */
  private static class ComplexTypeHelper {

    public final Field complexField;

    public int complexFieldStart;
    public int complexFieldLength;
    public int complexFieldEnd;

    public int fieldPosition;

    public ComplexTypeHelper(Field complexField) {
      this.complexField = complexField;
    }

    public void setCurrentFieldInfo(int complexFieldStart, int complexFieldLength) {
      this.complexFieldStart = complexFieldStart;
      this.complexFieldLength = complexFieldLength;
      complexFieldEnd = complexFieldStart + complexFieldLength;
      fieldPosition = complexFieldStart;
    }
  }

  private static class ListComplexTypeHelper extends ComplexTypeHelper {

    public Field elementField;

    public ListComplexTypeHelper(Field complexField, Field elementField) {
      super(complexField);
      this.elementField = elementField;
    }
  }

  private static class MapComplexTypeHelper extends ComplexTypeHelper {

    public Field keyField;
    public Field valueField;

    public boolean fieldHaveParsedKey;

    public MapComplexTypeHelper(Field complexField, Field keyField, Field valueField) {
      super(complexField);
      this.keyField = keyField;
      this.valueField = valueField;
      fieldHaveParsedKey = false;
    }
  }

  private static class StructComplexTypeHelper extends ComplexTypeHelper {

    public Field[] fields;

    public int nextFieldIndex;

    public StructComplexTypeHelper(Field complexField, Field[] fields) {
      super(complexField);
      this.fields = fields;
      nextFieldIndex = 0;
    }
  }

  private static class UnionComplexTypeHelper extends ComplexTypeHelper {

    public Field tagField;
    public Field[] fields;

    public boolean fieldHaveParsedTag;
    public int fieldTag;

    public UnionComplexTypeHelper(Field complexField, Field[] fields) {
      super(complexField);
      this.tagField = new Field(TypeInfoFactory.intTypeInfo);
      this.fields = fields;
      fieldHaveParsedTag = false;
    }
  }

  private int[] startPositions;

  private final byte[] separators;
  private final boolean isEscaped;
  private final byte escapeChar;
  private final int[] escapeCounts;
  private final byte[] nullSequenceBytes;
  private final boolean isExtendedBooleanLiteral;

  private final int fieldCount;
  private final Field[] fields;
  private final int maxLevelDepth;

  private byte[] bytes;
  private int start;
  private int end;
  private boolean topLevelParsed;

  // Used by readNextField/skipNextField and not by readField.
  private int nextFieldIndex;

  // For getDetailedReadPositionString.
  private int currentLevel;
  private int currentTopLevelFieldIndex;
  private int currentFieldStart;
  private int currentFieldLength;
  private int currentEscapeCount;

  private ComplexTypeHelper[] currentComplexTypeHelpers;

  // For string/char/varchar buffering when there are escapes.
  private int internalBufferLen;
  private byte[] internalBuffer;

  private final TimestampParser timestampParser;

  private boolean isEndOfInputReached;

  private int addComplexFields(List<TypeInfo> fieldTypeInfoList, Field[] fields, int depth) {
    Field field;
    final int count = fieldTypeInfoList.size();
    for (int i = 0; i < count; i++) {
      field = new Field(fieldTypeInfoList.get(i));
      if (!field.isPrimitive) {
        depth = Math.max(depth, addComplexTypeHelper(field, depth));
      }
      fields[i] = field;
    }
    return depth;
  }

  private int addComplexTypeHelper(Field complexField, int depth) {

    // Assume one separator (depth) needed.
    depth++;

    switch (complexField.complexCategory) {
    case LIST:
      {
        final ListTypeInfo listTypeInfo = (ListTypeInfo) complexField.typeInfo;
        final Field elementField = new Field(listTypeInfo.getListElementTypeInfo());
        if (!elementField.isPrimitive) {
          depth = addComplexTypeHelper(elementField, depth);
        }
        final ListComplexTypeHelper listHelper =
            new ListComplexTypeHelper(complexField, elementField);
        complexField.complexTypeHelper = listHelper;
      }
      break;
    case MAP:
      {
        // Map needs two separators (key and key/value pair).
        depth++;

        final MapTypeInfo mapTypeInfo = (MapTypeInfo) complexField.typeInfo;
        final Field keyField = new Field(mapTypeInfo.getMapKeyTypeInfo());
        if (!keyField.isPrimitive) {
          depth = Math.max(depth, addComplexTypeHelper(keyField, depth));
        }
        final Field valueField = new Field(mapTypeInfo.getMapValueTypeInfo());
        if (!valueField.isPrimitive) {
          depth = Math.max(depth, addComplexTypeHelper(valueField, depth));
        }
        final MapComplexTypeHelper mapHelper =
            new MapComplexTypeHelper(complexField, keyField, valueField);
        complexField.complexTypeHelper = mapHelper;
      }
      break;
    case STRUCT:
      {
        final StructTypeInfo structTypeInfo = (StructTypeInfo) complexField.typeInfo;
        final List<TypeInfo> fieldTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
        final Field[] fields = new Field[fieldTypeInfoList.size()];
        depth = addComplexFields(fieldTypeInfoList, fields, depth);
        final StructComplexTypeHelper structHelper =
            new StructComplexTypeHelper(complexField, fields);
        complexField.complexTypeHelper = structHelper;
      }
      break;
    case UNION:
      {
        final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) complexField.typeInfo;
        final List<TypeInfo> fieldTypeInfoList = unionTypeInfo.getAllUnionObjectTypeInfos();
        final Field[] fields = new Field[fieldTypeInfoList.size()];
        depth = addComplexFields(fieldTypeInfoList, fields, depth);
        final UnionComplexTypeHelper structHelper =
            new UnionComplexTypeHelper(complexField, fields);
        complexField.complexTypeHelper = structHelper;
      }
      break;
    default:
      throw new Error("Unexpected complex category " + complexField.complexCategory);
    }
    return depth;
  }

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos,
      DataTypePhysicalVariation[] dataTypePhysicalVariations, boolean useExternalBuffer,
      LazySerDeParameters lazyParams) {
    super(typeInfos, dataTypePhysicalVariations, useExternalBuffer);

    final int count = typeInfos.length;
    fieldCount = count;
    int depth = 0;
    fields = new Field[count];
    Field field;
    for (int i = 0; i < count; i++) {
      field = new Field(typeInfos[i], this.dataTypePhysicalVariations[i]);
      if (!field.isPrimitive) {
        depth = Math.max(depth, addComplexTypeHelper(field, 0));
      }
      fields[i] = field;
    }
    maxLevelDepth = depth;
    currentComplexTypeHelpers = new ComplexTypeHelper[depth];

    // Field length is difference between positions hence one extra.
    startPositions = new int[count + 1];

    this.separators = lazyParams.getSeparators();

    isEscaped = lazyParams.isEscaped();
    if (isEscaped) {
      escapeChar = lazyParams.getEscapeChar();
      escapeCounts = new int[count];
    } else {
      escapeChar = (byte) 0;
      escapeCounts = null;
    }
    nullSequenceBytes = lazyParams.getNullSequence().getBytes();
    isExtendedBooleanLiteral = lazyParams.isExtendedBooleanLiteral();
    if (lazyParams.isLastColumnTakesRest()) {
      throw new RuntimeException("serialization.last.column.takes.rest not supported");
    }

    timestampParser = new TimestampParser();

    internalBufferLen = -1;
  }

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer,
      LazySerDeParameters lazyParams) {
    this(typeInfos, null, useExternalBuffer, lazyParams);
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    start = offset;
    end = offset + length;
    topLevelParsed = false;
    currentLevel = 0;
    nextFieldIndex = -1;
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
    sb.append(fieldCount);
    sb.append(" fields with types ");
    sb.append(Arrays.toString(typeInfos));
    sb.append(".  ");
    if (!topLevelParsed) {
      sb.append("Error during field separator parsing");
    } else {
      sb.append("Read field #");
      sb.append(currentTopLevelFieldIndex);
      sb.append(" at field start position ");
      sb.append(startPositions[currentTopLevelFieldIndex]);
      int currentFieldLength = startPositions[currentTopLevelFieldIndex + 1] -
          startPositions[currentTopLevelFieldIndex] - 1;
      sb.append(" for field length ");
      sb.append(currentFieldLength);
    }

    return sb.toString();
  }

  /**
   * Parse the byte[] and fill each field.
   *
   * This is an adapted version of the parse method in the LazyStruct class.
   * They should parse things the same way.
   */
  private void topLevelParse() {

    int fieldId = 0;
    int fieldByteBegin = start;
    int fieldByteEnd = start;

    final byte separator = this.separators[0];
    final int fieldCount = this.fieldCount;
    final int[] startPositions = this.startPositions;
    final byte[] bytes = this.bytes;
    final int end = this.end;

    /*
     * Optimize the loops by pulling special end cases and global decisions like isEscaped out!
     */
    if (!isEscaped) {
      while (fieldByteEnd < end) {
        if (bytes[fieldByteEnd] == separator) {
          startPositions[fieldId++] = fieldByteBegin;
          if (fieldId == fieldCount) {
            break;
          }
          fieldByteBegin = ++fieldByteEnd;
        } else {
          fieldByteEnd++;
        }
      }
      // End serves as final separator.
      if (fieldByteEnd == end && fieldId < fieldCount) {
        startPositions[fieldId++] = fieldByteBegin;
      }
    } else {
      final byte escapeChar = this.escapeChar;
      final int endLessOne = end - 1;
      final int[] escapeCounts = this.escapeCounts;
      int escapeCount = 0;
      // Process the bytes that can be escaped (the last one can't be).
      while (fieldByteEnd < endLessOne) {
        if (bytes[fieldByteEnd] == separator) {
          escapeCounts[fieldId] = escapeCount;
          escapeCount = 0;
          startPositions[fieldId++] = fieldByteBegin;
          if (fieldId == fieldCount) {
            break;
          }
          fieldByteBegin = ++fieldByteEnd;
        } else if (bytes[fieldByteEnd] == escapeChar) {
          // Ignore the char after escape_char
          fieldByteEnd += 2;
          escapeCount++;
        } else {
          fieldByteEnd++;
        }
      }
      // Process the last byte if necessary.
      if (fieldByteEnd == endLessOne && fieldId < fieldCount) {
        if (bytes[fieldByteEnd] == separator) {
          escapeCounts[fieldId] = escapeCount;
          escapeCount = 0;
          startPositions[fieldId++] = fieldByteBegin;
          if (fieldId <= fieldCount) {
            fieldByteBegin = ++fieldByteEnd;
          }
        } else {
          fieldByteEnd++;
        }
      }
      // End serves as final separator.
      if (fieldByteEnd == end && fieldId < fieldCount) {
        escapeCounts[fieldId] = escapeCount;
        startPositions[fieldId++] = fieldByteBegin;
      }
    }

    if (fieldId == fieldCount || fieldByteEnd == end) {
      // All fields have been parsed, or bytes have been parsed.
      // We need to set the startPositions of fields.length to ensure we
      // can use the same formula to calculate the length of each field.
      // For missing fields, their starting positions will all be the same,
      // which will make their lengths to be -1 and uncheckedGetField will
      // return these fields as NULLs.
      Arrays.fill(startPositions, fieldId, startPositions.length, fieldByteEnd + 1);
    }

    isEndOfInputReached = (fieldByteEnd == end);
  }

  private int parseComplexField(int start, int end, int level) {

    if (start == end + 1) {

      // Data prematurely ended. Return start - 1 so we don't move our field position.
      return start - 1;
    }

    final byte separator = separators[level];
    int fieldByteEnd = start;

    final byte[] bytes = this.bytes;

    currentEscapeCount = 0;
    if (!isEscaped) {
      while (fieldByteEnd < end) {
        if (bytes[fieldByteEnd] == separator) {
          return fieldByteEnd;
        }
        fieldByteEnd++;
      }
    } else {
      final byte escapeChar = this.escapeChar;
      final int endLessOne = end - 1;
      int escapeCount = 0;
      // Process the bytes that can be escaped (the last one can't be).
      while (fieldByteEnd < endLessOne) {
        if (bytes[fieldByteEnd] == separator) {
          currentEscapeCount = escapeCount;
          return fieldByteEnd;
        } else if (bytes[fieldByteEnd] == escapeChar) {
          // Ignore the char after escape_char
          fieldByteEnd += 2;
          escapeCount++;
        } else {
          fieldByteEnd++;
        }
      }
      // Process the last byte.
      if (fieldByteEnd == endLessOne) {
        if (bytes[fieldByteEnd] != separator) {
          fieldByteEnd++;
        }
      }
      currentEscapeCount = escapeCount;
    }
    return fieldByteEnd;
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
    if (nextFieldIndex + 1 >= fieldCount) {
      return false;
    }
    nextFieldIndex++;
    return readField(nextFieldIndex);
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    if (!topLevelParsed) {
      topLevelParse();
      topLevelParsed = true;
    }
    if (nextFieldIndex + 1 >= fieldCount) {
      // No more.
    } else {
      nextFieldIndex++;
    }
  }

  @Override
  public boolean isReadFieldSupported() {
    return true;
  }

  private boolean checkNull(byte[] bytes, int start, int len) {
    if (len != nullSequenceBytes.length) {
      return false;
    }
    final byte[] nullSequenceBytes = this.nullSequenceBytes;
    switch(len) {
    case 0:
      return true;
    case 2:
      return bytes[start] == nullSequenceBytes[0] && bytes[start+1] == nullSequenceBytes[1];
    case 4:
      return bytes[start] == nullSequenceBytes[0] && bytes[start+1] == nullSequenceBytes[1]
          && bytes[start+2] == nullSequenceBytes[2] && bytes[start+3] == nullSequenceBytes[3];
    default:
      for (int i = 0; i < nullSequenceBytes.length; i++) {
        if (bytes[start + i] != nullSequenceBytes[i]) {
          return false;
        }
      }
      return true;
    }
  }

  /*
   * When supported, read a field by field number (i.e. random access).
   *
   * Currently, only LazySimpleDeserializeRead supports this.
   *
   * @return  Return true when the field was not null and data is put in the appropriate
   *          current* member.
   *          Otherwise, false when the field is null.
   */
  public boolean readField(int fieldIndex) throws IOException {

    Preconditions.checkState(currentLevel == 0);

    if (!topLevelParsed) {
      topLevelParse();
      topLevelParsed = true;
    }

    // Top level.
    currentTopLevelFieldIndex = fieldIndex;

    currentFieldStart = startPositions[fieldIndex];
    currentFieldLength = startPositions[fieldIndex + 1] - startPositions[fieldIndex] - 1;
    currentEscapeCount = (isEscaped ? escapeCounts[fieldIndex] : 0);
 
    return doReadField(fields[fieldIndex]);
  }

  private boolean doReadField(Field field) {
    final int fieldStart = currentFieldStart;
    final int fieldLength = currentFieldLength;
    if (fieldLength < 0) {
      return false;
    }

    final byte[] bytes = this.bytes;

    // Is the field the configured string representing NULL?
    if (nullSequenceBytes != null) {
      if (checkNull(bytes, fieldStart, fieldLength)) {
        return false;
      }
    }

    try {
      /*
       * We have a field and are positioned to it.  Read it.
       */
      if (field.isPrimitive) {
        switch (field.primitiveCategory) {
        case BOOLEAN:
          {
            int i = fieldStart;
            if (fieldLength == 4) {
              if ((bytes[i] == 'T' || bytes[i] == 't') &&
                  (bytes[i + 1] == 'R' || bytes[i + 1] == 'r') &&
                  (bytes[i + 2] == 'U' || bytes[i + 2] == 'u') &&
                  (bytes[i + 3] == 'E' || bytes[i + 3] == 'e')) {
                currentBoolean = true;
              } else {
                // No boolean value match for 4 char field.
                return false;
              }
            } else if (fieldLength == 5) {
              if ((bytes[i] == 'F' || bytes[i] == 'f') &&
                  (bytes[i + 1] == 'A' || bytes[i + 1] == 'a') &&
                  (bytes[i + 2] == 'L' || bytes[i + 2] == 'l') &&
                  (bytes[i + 3] == 'S' || bytes[i + 3] == 's') &&
                  (bytes[i + 4] == 'E' || bytes[i + 4] == 'e')) {
                currentBoolean = false;
              } else {
                // No boolean value match for 5 char field.
                return false;
              }
            } else if (isExtendedBooleanLiteral && fieldLength == 1) {
              byte b = bytes[fieldStart];
              if (b == '1' || b == 't' || b == 'T') {
                currentBoolean = true;
              } else if (b == '0' || b == 'f' || b == 'F') {
                currentBoolean = false;
              } else {
                // No boolean value match for extended 1 char field.
                return false;
              }
            } else {
              // No boolean value match for other lengths.
              return false;
            }
          }
          return true;
        case BYTE:
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentByte = LazyByte.parseByte(bytes, fieldStart, fieldLength, 10);
          return true;
        case SHORT:
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentShort = LazyShort.parseShort(bytes, fieldStart, fieldLength, 10);
          return true;
        case INT:
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentInt = LazyInteger.parseInt(bytes, fieldStart, fieldLength, 10);
          return true;
        case LONG:
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentLong = LazyLong.parseLong(bytes, fieldStart, fieldLength, 10);
          return true;
        case FLOAT:
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentFloat =
              Float.parseFloat(
                  new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8));
          return true;
        case DOUBLE:
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentDouble = StringToDouble.strtod(bytes, fieldStart, fieldLength);
          return true;
        case STRING:
        case CHAR:
        case VARCHAR:
          {
            if (isEscaped) {
              if (currentEscapeCount == 0) {
                // No escaping.
                currentExternalBufferNeeded = false;
                currentBytes = bytes;
                currentBytesStart = fieldStart;
                currentBytesLength = fieldLength;
              } else {
                final int unescapedLength = fieldLength - currentEscapeCount;
                if (useExternalBuffer) {
                  currentExternalBufferNeeded = true;
                  currentExternalBufferNeededLen = unescapedLength;
                } else {
                  // The copyToBuffer will reposition and re-read the input buffer.
                  currentExternalBufferNeeded = false;
                  if (internalBufferLen < unescapedLength) {
                    internalBufferLen = unescapedLength;
                    internalBuffer = new byte[internalBufferLen];
                  }
                  copyToBuffer(internalBuffer, 0, unescapedLength);
                  currentBytes = internalBuffer;
                  currentBytesStart = 0;
                  currentBytesLength = unescapedLength;
                }
              }
            } else {
              // If the data is not escaped, reference the data directly.
              currentExternalBufferNeeded = false;
              currentBytes = bytes;
              currentBytesStart = fieldStart;
              currentBytesLength = fieldLength;
            }
          }
          return true;
        case BINARY:
          {
            byte[] recv = new byte[fieldLength];
            System.arraycopy(bytes, fieldStart, recv, 0, fieldLength);
            byte[] decoded = LazyBinary.decodeIfNeeded(recv);
            // use the original bytes in case decoding should fail
            decoded = decoded.length > 0 ? decoded : recv;
            currentBytes = decoded;
            currentBytesStart = 0;
            currentBytesLength = decoded.length;
          }
          return true;
        case DATE:
          if (!LazyUtils.isDateMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          currentDateWritable.set(
              Date.valueOf(
                  new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8)));
          return true;
        case TIMESTAMP:
          {
            if (!LazyUtils.isDateMaybe(bytes, fieldStart, fieldLength)) {
              return false;
            }
            String s = new String(bytes, fieldStart, fieldLength, StandardCharsets.US_ASCII);
            if (s.compareTo("NULL") == 0) {
              logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
              return false;
            }
            try {
              currentTimestampWritable.set(timestampParser.parseTimestamp(s));
            } catch (IllegalArgumentException e) {
              logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
              return false;
            }
          }
          return true;
        case INTERVAL_YEAR_MONTH:
          if (fieldLength == 0) {
            return false;
          }
          try {
            String s = new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8);
            currentHiveIntervalYearMonthWritable.set(HiveIntervalYearMonth.valueOf(s));
          } catch (Exception e) {
            logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_YEAR_MONTH");
            return false;
          }
          return true;
        case INTERVAL_DAY_TIME:
          if (fieldLength == 0) {
            return false;
          }
          try {
            String s = new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8);
            currentHiveIntervalDayTimeWritable.set(HiveIntervalDayTime.valueOf(s));
          } catch (Exception e) {
            logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_DAY_TIME");
            return false;
          }
          return true;
        case DECIMAL:
          {
            if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
              return false;
            }
            // Trim blanks because OldHiveDecimal did...
            currentHiveDecimalWritable.setFromBytes(bytes, fieldStart, fieldLength, /* trimBlanks */ true);
            boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
            if (!decimalIsNull) {
              DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) field.typeInfo;

              int precision = decimalTypeInfo.getPrecision();
              int scale = decimalTypeInfo.getScale();

              decimalIsNull = !currentHiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
              if (!decimalIsNull) {
                if (field.dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {
                  currentDecimal64 = currentHiveDecimalWritable.serialize64(scale);
                }
                return true;
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Data not in the HiveDecimal data type range so converted to null. Given data is :"
                  + new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8));
            }
          }
          return false;

        default:
          throw new Error("Unexpected primitive category " + field.primitiveCategory);
        }
      } else {
        switch (field.complexCategory) {
        case LIST:
        case MAP:
        case STRUCT:
        case UNION:
          {
            if (currentLevel > 0) {
  
              // Check for Map which occupies 2 levels (key separator and key/value pair separator).
              if (currentComplexTypeHelpers[currentLevel - 1] == null) {
                Preconditions.checkState(currentLevel > 1);
                Preconditions.checkState(
                    currentComplexTypeHelpers[currentLevel - 2] instanceof MapComplexTypeHelper);
                currentLevel++;
              }
            }
            ComplexTypeHelper complexTypeHelper = field.complexTypeHelper; 
            currentComplexTypeHelpers[currentLevel++] = complexTypeHelper;
            if (field.complexCategory == Category.MAP) {
              currentComplexTypeHelpers[currentLevel] = null;
            }
  
            // Set up context for readNextComplexField.
            complexTypeHelper.setCurrentFieldInfo(currentFieldStart, currentFieldLength);
          }
          return true;
        default:
          throw new Error("Unexpected complex category " + field.complexCategory);
        }
      }
    } catch (NumberFormatException nfe) {
       logExceptionMessage(bytes, fieldStart, fieldLength, field.complexCategory, field.primitiveCategory);
       return false;
    } catch (IllegalArgumentException iae) {
      logExceptionMessage(bytes, fieldStart, fieldLength, field.complexCategory, field.primitiveCategory);
      return false;
    }
  }

  @Override
  public void copyToExternalBuffer(byte[] externalBuffer, int externalBufferStart) {
    copyToBuffer(externalBuffer, externalBufferStart, currentExternalBufferNeededLen);
  }

  private void copyToBuffer(byte[] buffer, int bufferStart, int bufferLength) {

    final int fieldStart = currentFieldStart;
    final int fieldLength = currentFieldLength;
    int k = 0;
    for (int i = 0; i < fieldLength; i++) {
      byte b = bytes[fieldStart + i];
      if (b == escapeChar && i < fieldLength - 1) {
        ++i;
        // Check if it's '\r' or '\n'
        if (bytes[fieldStart + i] == 'r') {
          buffer[bufferStart + k++] = '\r';
        } else if (bytes[fieldStart + i] == 'n') {
          buffer[bufferStart + k++] = '\n';
        } else {
          // get the next byte
          buffer[bufferStart + k++] = bytes[fieldStart + i];
        }
      } else {
        buffer[bufferStart + k++] = b;
      }
    }
  }

  @Override
  public boolean isNextComplexMultiValue() {
    Preconditions.checkState(currentLevel > 0);

    final ComplexTypeHelper complexTypeHelper = currentComplexTypeHelpers[currentLevel - 1];
    final Field complexField = complexTypeHelper.complexField;
    final int fieldPosition = complexTypeHelper.fieldPosition;
    final int complexFieldEnd = complexTypeHelper.complexFieldEnd;
    switch (complexField.complexCategory) {
    case LIST:
      {
        // Allow for empty string, etc.
        final ListComplexTypeHelper listHelper = (ListComplexTypeHelper) complexTypeHelper;
        final boolean isElementStringFamily;
        final Field elementField = listHelper.elementField;
        if (elementField.isPrimitive) {
          switch (elementField.primitiveCategory) {
            case STRING:
            case VARCHAR:
            case CHAR:
              isElementStringFamily = true;
              break;
            default:
              isElementStringFamily = false;
              break;
          }
        } else {
          isElementStringFamily = false;
        }
        final boolean isNext;
        if (isElementStringFamily) {
          isNext = (fieldPosition <= complexFieldEnd);
        } else {
          isNext = (fieldPosition < complexFieldEnd);
        }
        if (!isNext) {
          popComplexType();
        }
        return isNext;
      }
    case MAP:
      {
        final boolean isNext = (fieldPosition < complexFieldEnd);
        if (!isNext) {
          popComplexType();
        }
        return isNext;
      }
    case STRUCT:
    case UNION:
      throw new Error("Complex category " + complexField.complexCategory + " not multi-value");
    default:
      throw new Error("Unexpected complex category " + complexField.complexCategory);
    }
  }

  private void popComplexType() {
    Preconditions.checkState(currentLevel > 0);
    currentLevel--;
    if (currentLevel > 0) {

      // Check for Map which occupies 2 levels (key separator and key/value pair separator).
      if (currentComplexTypeHelpers[currentLevel - 1] == null) {
        Preconditions.checkState(currentLevel > 1);
        Preconditions.checkState(
            currentComplexTypeHelpers[currentLevel - 2] instanceof MapComplexTypeHelper);
        currentLevel--;
      }
    }
  }

  /*
   * NOTE: There is an expectation that all fields will be read-thru.
   */
  @Override
  public boolean readComplexField() throws IOException {

    Preconditions.checkState(currentLevel > 0);

    final ComplexTypeHelper complexTypeHelper = currentComplexTypeHelpers[currentLevel - 1];
    final Field complexField = complexTypeHelper.complexField;
    switch (complexField.complexCategory) {
    case LIST:
      {
        final ListComplexTypeHelper listHelper = (ListComplexTypeHelper) complexTypeHelper;
        final int fieldPosition = listHelper.fieldPosition;
        final int complexFieldEnd = listHelper.complexFieldEnd;

        // When data is prematurely ended the fieldPosition will be 1 more than the end.
        Preconditions.checkState(fieldPosition <= complexFieldEnd + 1);

        final int fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel);
        listHelper.fieldPosition = fieldEnd + 1;  // Move past separator.

        currentFieldStart = fieldPosition;
        currentFieldLength = fieldEnd - fieldPosition;

        return doReadField(listHelper.elementField);
      }
    case MAP:
      {
        final MapComplexTypeHelper mapHelper = (MapComplexTypeHelper) complexTypeHelper;
        final int fieldPosition = mapHelper.fieldPosition;
        final int complexFieldEnd = mapHelper.complexFieldEnd;

        // When data is prematurely ended the fieldPosition will be 1 more than the end.
        Preconditions.checkState(fieldPosition <= complexFieldEnd + 1);
  
        currentFieldStart = fieldPosition;

        final boolean isParentMap = isParentMap();
        if (isParentMap) {
          currentLevel++;
        }
        int fieldEnd;
        if (!mapHelper.fieldHaveParsedKey) {

          // Parse until key separator (currentLevel + 1).
          fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel + 1);

          mapHelper.fieldPosition = fieldEnd + 1;  // Move past key separator.

          currentFieldLength = fieldEnd - fieldPosition;

          mapHelper.fieldHaveParsedKey = true;
          final boolean result = doReadField(mapHelper.keyField);
          if (isParentMap) {
            currentLevel--;
          }
          return result;
        } else {

          // Parse until pair separator (currentLevel).
          fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel);

          mapHelper.fieldPosition = fieldEnd + 1;  // Move past pair separator.

          currentFieldLength = fieldEnd - fieldPosition;

          mapHelper.fieldHaveParsedKey = false;
          final boolean result = doReadField(mapHelper.valueField);
          if (isParentMap) {
            currentLevel--;
          }
          return result;
        }
      }
    case STRUCT:
      {
        final StructComplexTypeHelper structHelper = (StructComplexTypeHelper) complexTypeHelper;
        final int fieldPosition = structHelper.fieldPosition;
        final int complexFieldEnd = structHelper.complexFieldEnd;

        // When data is prematurely ended the fieldPosition will be 1 more than the end.
        Preconditions.checkState(fieldPosition <= complexFieldEnd + 1);

        currentFieldStart = fieldPosition;

        final int nextFieldIndex = structHelper.nextFieldIndex;
        final Field[] fields = structHelper.fields;
        final int fieldEnd;
        if (nextFieldIndex != fields.length - 1) {

          // Parse until field separator (currentLevel).
          fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel);

          structHelper.fieldPosition = fieldEnd + 1;  // Move past parent field separator.

          currentFieldLength = fieldEnd - fieldPosition;

          return doReadField(fields[structHelper.nextFieldIndex++]);
        } else {

          // Parse until field separator (currentLevel).
          fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel);
          currentFieldLength = fieldEnd - fieldPosition;

          structHelper.nextFieldIndex = 0;
          boolean result = doReadField(fields[fields.length - 1]);

          if (!isEscaped) {

            // No parsing necessary -- the end is the parent's end.
            structHelper.fieldPosition = complexFieldEnd + 1;  // Move past parent field separator.
            currentEscapeCount = 0;
          } else {
            // We must parse to get the escape count.
            parseComplexField(fieldPosition, complexFieldEnd, currentLevel - 1);
          }

          return result;
        }
      }
    case UNION:
      {
        final UnionComplexTypeHelper unionHelper = (UnionComplexTypeHelper) complexTypeHelper;
        final int fieldPosition = unionHelper.fieldPosition;
        final int complexFieldEnd = unionHelper.complexFieldEnd;

        // When data is prematurely ended the fieldPosition will be 1 more than the end.
        Preconditions.checkState(fieldPosition <= complexFieldEnd + 1);

        currentFieldStart = fieldPosition;

        final int fieldEnd;
        if (!unionHelper.fieldHaveParsedTag) {
          boolean isParentMap = isParentMap();
          if (isParentMap) {
            currentLevel++;
          }

          // Parse until union separator (currentLevel).
          fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel);

          unionHelper.fieldPosition = fieldEnd + 1;  // Move past union separator.

          currentFieldLength = fieldEnd - fieldPosition;

          unionHelper.fieldHaveParsedTag = true;
          boolean successful = doReadField(unionHelper.tagField);
          if (!successful) {
            throw new IOException("Null union tag");
          }
          unionHelper.fieldTag = currentInt;

          if (isParentMap) {
            currentLevel--;
          }
          return true;
        } else {

          if (!isEscaped) {

            // No parsing necessary -- the end is the parent's end.
            unionHelper.fieldPosition = complexFieldEnd + 1;  // Move past parent field separator.
            currentEscapeCount = 0;
          } else {
            // We must parse to get the escape count.
            fieldEnd = parseComplexField(fieldPosition, complexFieldEnd, currentLevel - 1);
          }

          currentFieldLength = complexFieldEnd - fieldPosition;

          unionHelper.fieldHaveParsedTag = false;
          return doReadField(unionHelper.fields[unionHelper.fieldTag]);
        }
      }
    default:
      throw new Error("Unexpected complex category " + complexField.complexCategory);
    }
  }

  private boolean isParentMap() {
    return currentLevel >= 2 &&
        currentComplexTypeHelpers[currentLevel - 2] instanceof MapComplexTypeHelper;
  }

  @Override
  public void finishComplexVariableFieldsType() {
    Preconditions.checkState(currentLevel > 0);

    final ComplexTypeHelper complexTypeHelper = currentComplexTypeHelpers[currentLevel - 1];
    final Field complexField = complexTypeHelper.complexField;
    switch (complexField.complexCategory) {
    case LIST:
    case MAP:
      throw new Error("Complex category " + complexField.complexCategory + " is not variable fields type");
    case STRUCT:
    case UNION:
      popComplexType();
      break;
    default:
      throw new Error("Unexpected category " + complexField.complexCategory);
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
    return isEndOfInputReached;
  }

  public void logExceptionMessage(byte[] bytes, int bytesStart, int bytesLength,
      Category dataComplexCategory, PrimitiveCategory dataPrimitiveCategory) {
    final String dataType;
    if (dataComplexCategory == null) {
      switch (dataPrimitiveCategory) {
      case BYTE:
        dataType = "TINYINT";
        break;
      case LONG:
        dataType = "BIGINT";
        break;
      case SHORT:
        dataType = "SMALLINT";
        break;
      default:
        dataType = dataPrimitiveCategory.toString();
        break;
      }
    } else {
      switch (dataComplexCategory) {
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
        dataType = dataComplexCategory.toString();
        break;
      default:
        throw new Error("Unexpected complex category " + dataComplexCategory);
      }
    }
    logExceptionMessage(bytes, bytesStart, bytesLength, dataType);
  }

  public void logExceptionMessage(byte[] bytes, int bytesStart, int bytesLength, String dataType) {
    try {
      if (LOG.isDebugEnabled()) {
        String byteData = Text.decode(bytes, bytesStart, bytesLength);
        LOG.debug("Data not in the " + dataType
            + " data type range so converted to null. Given data is :" +
                    byteData, new Exception("For debugging purposes"));
      }
    } catch (CharacterCodingException e1) {
      LOG.debug("Data not in the " + dataType + " data type range so converted to null.", e1);
    }
  }

  //------------------------------------------------------------------------------------------------

  private static final byte[] maxLongBytes = ((Long) Long.MAX_VALUE).toString().getBytes();

  public static int byteArrayCompareRanges(byte[] arg1, int start1, byte[] arg2, int start2, int len) {
    for (int i = 0; i < len; i++) {
      // Note the "& 0xff" is just a way to convert unsigned bytes to signed integer.
      int b1 = arg1[i + start1] & 0xff;
      int b2 = arg2[i + start2] & 0xff;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return 0;
  }

}
